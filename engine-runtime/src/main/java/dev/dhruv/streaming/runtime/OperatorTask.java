package dev.dhruv.streaming.runtime;

import dev.dhruv.streaming.api.CheckpointBarrier;
import dev.dhruv.streaming.api.Operator;
import dev.dhruv.streaming.api.StreamElement;
import dev.dhruv.streaming.api.StreamRecord;
import dev.dhruv.streaming.api.Watermark;
import dev.dhruv.streaming.api.metrics.Counter;
import dev.dhruv.streaming.runtime.metrics.TaskMetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * One running instance of one operator: a thread, an inbound queue, and the loop between them.
 *
 * <p>This class is the heart of the engine, and it barely changes from here. Phase 3 fills in
 * the watermark branch, Phase 4 the barrier branch; the shape below is already the final one.
 * That is worth setting up now rather than later, because the shape is the lesson: control
 * elements and data records arrive on the same channel, in order, and are dispatched by the
 * same switch.
 *
 * <h2>Single-threaded on purpose</h2>
 *
 * <p>Exactly one thread ever runs an operator. Everything an operator touches -- its fields,
 * its state, its collector -- is therefore free of concurrency, and no user code in this
 * project needs a lock or a synchronised block. Parallelism comes from running more subtasks,
 * not from more threads inside one. Giving that up would buy very little and would make keyed
 * state, timers and snapshotting all significantly harder to reason about.
 *
 * <h2>Why a queue</h2>
 *
 * <p>The inbound queue is bounded. When this task falls behind, its queue fills, and whichever
 * task is producing into it blocks. That is the entire backpressure mechanism, and it works
 * because nobody tried to be clever: no dropping, no unbounded buffering, no separate signal.
 */
final class OperatorTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(OperatorTask.class);

    /**
     * How long to wait on an empty queue before looking around.
     *
     * <p>The loop must regain control periodically even when no records are arriving, so it can
     * notice cancellation. From Phase 3 it also needs to wake up to emit watermarks on a
     * schedule, which an indefinitely blocking take would prevent.
     */
    private static final long POLL_TIMEOUT_MILLIS = 100;

    private final String taskId;
    private final Operator<Object, Object> operator;
    private final BlockingQueue<StreamElement> inputQueue;
    private final Output output;
    private final OutputCollector<Object> collector;
    private final RuntimeOperatorContext context;
    private final TaskMetricGroup metrics;
    private final Counter recordsIn;

    /**
     * How many upstream subtasks send to this one. A task is finished when it has seen an
     * end-of-stream watermark from every one of them.
     */
    private final int upstreamSubtaskCount;

    private int endOfStreamMarkersSeen;
    private volatile boolean running = true;

    @SuppressWarnings("unchecked")
    OperatorTask(String taskId,
                 Operator<?, ?> operator,
                 BlockingQueue<StreamElement> inputQueue,
                 Output output,
                 int upstreamSubtaskCount,
                 TaskMetricGroup metrics) {
        this.taskId = taskId;
        this.upstreamSubtaskCount = upstreamSubtaskCount;
        this.operator = (Operator<Object, Object>) operator;
        this.inputQueue = inputQueue;
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
                StreamElement element = inputQueue.poll(POLL_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                if (element == null) {
                    continue;
                }

                switch (element) {
                    case StreamRecord<?> record -> processRecord(record);
                    case Watermark watermark -> handleWatermark(watermark);
                    case CheckpointBarrier barrier -> handleBarrier(barrier);
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
     * <p>It will track one watermark per input channel, take the minimum across the non-idle
     * ones as this task's event-time clock, fire any timers that clock has now reached, and
     * broadcast the result downstream.
     *
     * <p>What is implemented here is only the end-of-stream case, because a task has to be able
     * to finish. {@link Watermark#MAX} arriving from an upstream subtask means that subtask will
     * send nothing further, and once every upstream subtask has said so, this one is done.
     *
     * <p>Counting them is not optional, and the reason is the shape of Phase 3's problem in
     * miniature: a task with four upstream subtasks that stopped at the first end-of-stream
     * marker would abandon the records the other three still had in flight. A watermark is a
     * claim about one channel, and a task's own state can only follow from all of its channels
     * together. Phase 3 generalises this counter into a per-channel watermark array; the
     * minimum rule is the same idea applied continuously rather than once.
     */
    private void handleWatermark(Watermark watermark) throws Exception {
        if (watermark.timestamp() != Watermark.MAX.timestamp()) {
            output.broadcast(watermark);
            return;
        }

        endOfStreamMarkersSeen++;
        if (endOfStreamMarkersSeen < upstreamSubtaskCount) {
            return;
        }
        // Every upstream subtask has finished. Pass the news on and stop.
        output.broadcast(Watermark.MAX);
        running = false;
    }

    /**
     * Phase 4 fills this in.
     *
     * <p>It will align the barrier across every input channel, snapshot this task's state,
     * forward the barrier downstream, and only then acknowledge to the master. Nothing injects
     * barriers yet, so this branch cannot currently be reached; it exists so that the switch
     * above is already exhaustive over {@link StreamElement} and stays that way.
     */
    private void handleBarrier(CheckpointBarrier barrier) throws Exception {
        output.broadcast(barrier);
    }

    /**
     * Asks this task to stop after it finishes the element in hand.
     *
     * <p>Cooperative rather than an interrupt, so a task stops between records instead of
     * halfway through one. An interrupt follows only if the task does not notice in time.
     */
    void cancel() {
        running = false;
    }

    String taskId() {
        return taskId;
    }

    TaskMetricGroup metrics() {
        return metrics;
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
