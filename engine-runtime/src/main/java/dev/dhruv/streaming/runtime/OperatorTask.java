package dev.dhruv.streaming.runtime;

import dev.dhruv.streaming.api.CheckpointBarrier;
import dev.dhruv.streaming.api.Operator;
import dev.dhruv.streaming.api.StreamElement;
import dev.dhruv.streaming.api.StreamRecord;
import dev.dhruv.streaming.api.Watermark;
import dev.dhruv.streaming.api.metrics.Counter;
import dev.dhruv.streaming.runtime.metrics.TaskMetricGroup;
import dev.dhruv.streaming.runtime.transport.InputGate;
import dev.dhruv.streaming.runtime.transport.Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.concurrent.TimeUnit;

/**
 * One running instance of one operator: a thread, an input gate, and the loop between them.
 *
 * <p>This class is the heart of the engine, and it barely changes from here. Phase 3 fills in
 * the watermark branch, Phase 4 the barrier branch; the shape below is already the final one.
 * That is worth setting up now rather than later, because the shape is the lesson: control
 * elements and data records arrive on the same channels, in order, and are dispatched by the
 * same switch.
 *
 * <p>Nothing in this class changed when the engine became distributed. The gate it reads from
 * is fed by another thread in Phase 1 and by a gRPC stream in Phase 2; the output it writes to
 * is a queue handoff or a network write. Neither fact appears here, which is the only real test
 * of whether {@link InputGate} and {@link Output} were drawn in the right place.
 *
 * <h2>Single-threaded on purpose</h2>
 *
 * <p>Exactly one thread ever runs an operator. Everything an operator touches -- its fields, its
 * state, its collector -- is therefore free of concurrency, and no user code in this project
 * needs a lock or a synchronised block. Parallelism comes from running more subtasks, not from
 * more threads inside one. Giving that up would buy very little and would make keyed state,
 * timers and snapshotting all significantly harder to reason about.
 */
public final class OperatorTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(OperatorTask.class);

    /**
     * How long to wait on an empty gate before looking around.
     *
     * <p>The loop must regain control periodically even when no records are arriving, so it can
     * notice cancellation. From Phase 3 it also needs to wake up to emit watermarks on a
     * schedule, which an indefinitely blocking take would prevent.
     */
    private static final long POLL_TIMEOUT_MILLIS = 100;

    private final String taskId;
    private final Operator<Object, Object> operator;
    private final InputGate inputGate;
    private final Output output;
    private final OutputCollector<Object> collector;
    private final RuntimeOperatorContext context;
    private final TaskMetricGroup metrics;
    private final Counter recordsIn;

    private int endOfStreamMarkersSeen;
    private volatile boolean running = true;

    /**
     * What to do when this task fails. In Phase 1 nothing listens and a failure is only logged;
     * from Phase 2 the worker listens and reports to the master, which fails the job.
     */
    private BiConsumer<String, Throwable> failureListener = (taskId, failure) -> {
    };

    /**
     * Creates a task.
     *
     * @param taskId   identity for logs and for reporting failures to the master
     * @param operator the user logic, already a private copy for this subtask
     * @param inputGate where records arrive, one queue per upstream subtask
     * @param output   where results go
     * @param metrics  this subtask's metric group
     */
    @SuppressWarnings("unchecked")
    public OperatorTask(String taskId,
                        Operator<?, ?> operator,
                        InputGate inputGate,
                        Output output,
                        TaskMetricGroup metrics) {
        this.taskId = taskId;
        this.operator = (Operator<Object, Object>) operator;
        this.inputGate = inputGate;
        this.output = output;
        this.collector = new OutputCollector<>(output);
        this.context = new RuntimeOperatorContext(metrics);
        this.metrics = metrics;
        this.recordsIn = metrics.counter("records-in");
    }

    @Override
    public void run() {
        log.info("task {} starting", taskId);
        try {
            operator.open(context);

            while (running) {
                Optional<InputGate.IncomingElement> incoming =
                        inputGate.poll(POLL_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                if (incoming.isEmpty()) {
                    // Nothing arrived. Ship anything batched rather than let it wait for
                    // traffic that may not come.
                    output.flush();
                    continue;
                }

                StreamElement element = incoming.get().element();
                int channel = incoming.get().channelIndex();

                switch (element) {
                    case StreamRecord<?> record -> processRecord(record);
                    case Watermark watermark -> handleWatermark(watermark, channel);
                    case CheckpointBarrier barrier -> handleBarrier(barrier, channel);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("task {} interrupted, stopping", taskId);
        } catch (TaskCancelledException e) {
            log.info("task {} cancelled: {}", taskId, e.getMessage());
        } catch (Exception e) {
            // Phase 2 reports this to the master, which fails the job. Phase 4 makes that a
            // restart from the last completed checkpoint instead.
            log.error("task {} failed", taskId, e);
            failureListener.accept(taskId, e);
        } finally {
            closeQuietly();
            log.info("task {} stopped after {} records", taskId, recordsIn.count());
        }
    }

    private void processRecord(StreamRecord<?> record) throws Exception {
        // Phase 3 restores the key context here, once there is keyed state for it to scope:
        //     if (keySelector != null) {
        //         stateBackend.setCurrentKey(keySelector.getKey(record.value()));
        //     }
        collector.setCurrentTimestamp(record.timestamp());
        operator.processElement(asObjectRecord(record), collector);
        recordsIn.increment();
    }

    /**
     * Phase 3 fills this in.
     *
     * <p>It will keep one watermark per input channel -- which is why {@code channelIndex} is
     * already threaded through -- take the minimum across the non-idle ones as this task's
     * event-time clock, fire any timers that clock has now reached, and broadcast the result.
     *
     * <p>What is implemented here is only the end-of-stream case, because a task has to be able
     * to finish. {@link Watermark#MAX} arriving on a channel means that channel will carry
     * nothing further, and once every channel has said so, this task is done.
     *
     * <p>Counting them is not optional, and it is Phase 3's problem in miniature: a task with
     * four input channels that stopped at the first end-of-stream marker would abandon the
     * records the other three still had in flight. A watermark is a claim about one channel, and
     * a task's own clock can only follow from all of its channels together.
     */
    private void handleWatermark(Watermark watermark, int channelIndex) throws Exception {
        if (watermark.timestamp() != Watermark.MAX.timestamp()) {
            output.broadcast(watermark);
            return;
        }

        endOfStreamMarkersSeen++;
        if (endOfStreamMarkersSeen < inputGate.channelCount()) {
            return;
        }
        // Every channel has finished. Pass the news on and stop.
        output.broadcast(Watermark.MAX);
        output.flush();
        running = false;
    }

    /**
     * Phase 4 fills this in.
     *
     * <p>It will align the barrier across every input channel -- blocking each one as its
     * barrier arrives, which is what {@link InputGate#blockChannel} exists for -- then snapshot
     * this task's state, forward the barrier downstream, and only then acknowledge to the
     * master. Nothing injects barriers yet, so this branch cannot currently be reached; it
     * exists so that the switch above is already exhaustive over {@link StreamElement} and stays
     * that way.
     */
    private void handleBarrier(CheckpointBarrier barrier, int channelIndex) throws Exception {
        output.broadcast(barrier);
    }

    /**
     * Asks this task to stop after it finishes the element in hand.
     *
     * <p>Cooperative rather than an interrupt, so a task stops between records instead of
     * halfway through one. An interrupt follows only if the task does not notice in time.
     */
    public void cancel() {
        running = false;
    }

    /**
     * Returns this task's id.
     *
     * @return the task id
     */
    public String taskId() {
        return taskId;
    }

    /**
     * Returns this task's metrics.
     *
     * @return the metric group
     */
    public TaskMetricGroup metrics() {
        return metrics;
    }

    /**
     * Returns this task's input gate, so a worker can feed it from the network.
     *
     * @return the input gate
     */
    public InputGate inputGate() {
        return inputGate;
    }

    /**
     * Registers what to do when this task fails.
     *
     * @param listener called with the task id and the failure
     */
    public void onFailure(BiConsumer<String, Throwable> listener) {
        this.failureListener = listener;
    }

    private void closeQuietly() {
        try {
            operator.close();
        } catch (Exception e) {
            log.warn("task {} failed to close its operator cleanly", taskId, e);
        }
        output.close();
    }

    @SuppressWarnings("unchecked")
    private static StreamRecord<Object> asObjectRecord(StreamRecord<?> record) {
        return (StreamRecord<Object>) record;
    }
}
