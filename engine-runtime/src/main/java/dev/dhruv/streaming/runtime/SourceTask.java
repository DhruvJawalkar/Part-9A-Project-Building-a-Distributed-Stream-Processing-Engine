package dev.dhruv.streaming.runtime;

import dev.dhruv.streaming.api.Collector;
import dev.dhruv.streaming.api.Source;
import dev.dhruv.streaming.api.StreamRecord;
import dev.dhruv.streaming.api.TimestampAssigner;
import dev.dhruv.streaming.api.Watermark;
import dev.dhruv.streaming.api.metrics.Counter;
import dev.dhruv.streaming.runtime.metrics.TaskMetricGroup;
import dev.dhruv.streaming.runtime.transport.Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.BiConsumer;

/**
 * One running instance of one source.
 *
 * <p>A source task has no input gate, which is the one structural way it differs from every
 * other task: records originate here rather than arriving. So instead of a loop that waits on
 * inputs, it has a loop that asks the source for whatever it has.
 *
 * <p>The pull shape is what makes backpressure reach all the way to the broker. When the job
 * downstream slows, this task blocks inside {@code emit} -- on a full queue locally, or waiting
 * for credit across the network -- which means it stops calling {@link Source#poll}, which means
 * nothing is fetched. No signal has to travel backwards and no component has to decide what to
 * discard. The source simply is not asked.
 */
public final class SourceTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(SourceTask.class);

    private final String taskId;
    private final Source<Object> source;
    private final RuntimeSourceContext context;
    private final Output output;
    private final Optional<TimestampAssigner<Object>> timestampAssigner;
    private final Optional<BoundedOutOfOrdernessGenerator> watermarkGenerator;
    private final TaskMetricGroup metrics;
    private final Counter recordsOut;

    private volatile boolean running = true;

    /** See {@link OperatorTask}: nothing listens in Phase 1, the worker listens from Phase 2. */
    private BiConsumer<String, Throwable> failureListener = (taskId, failure) -> {
    };

    /**
     * Creates a source task.
     *
     * @param taskId            identity for logs and for reporting failures
     * @param source            the user's source, already a private copy for this subtask
     * @param context           what this subtask should read
     * @param output            where records go
     * @param timestampAssigner how to read event time out of a record, if configured
     * @param metrics           this subtask's metric group
     */
    @SuppressWarnings("unchecked")
    public SourceTask(String taskId,
                      Source<?> source,
                      RuntimeSourceContext context,
                      Output output,
                      Optional<? extends TimestampAssigner<?>> timestampAssigner,
                      TaskMetricGroup metrics) {
        this(taskId, source, context, output, timestampAssigner, 0, 0, metrics);
    }

    /**
     * Creates an event-time aware source task.
     *
     * <p>Idleness is emitted in band as a watermark status rather than inferred from an absent
     * watermark downstream. Absence is ambiguous on a distributed channel: it can mean a slow
     * sender, a congested network, or a genuinely idle source.
     */
    @SuppressWarnings("unchecked")
    public SourceTask(String taskId,
                      Source<?> source,
                      RuntimeSourceContext context,
                      Output output,
                      Optional<? extends TimestampAssigner<?>> timestampAssigner,
                      long outOfOrdernessMillis,
                      long idleTimeoutMillis,
                      TaskMetricGroup metrics) {
        this.taskId = taskId;
        this.source = (Source<Object>) source;
        this.context = context;
        this.output = output;
        this.timestampAssigner = (Optional<TimestampAssigner<Object>>) timestampAssigner;
        this.watermarkGenerator = this.timestampAssigner.map(ignored ->
                new BoundedOutOfOrdernessGenerator(outOfOrdernessMillis,
                        idleTimeoutMillis == 0 ? Long.MAX_VALUE : idleTimeoutMillis));
        this.metrics = metrics;
        this.recordsOut = metrics.counter("records-out");
    }

    @Override
    public void run() {
        log.info("source task {} starting", taskId);
        try {
            source.open(context);

            Collector<Object> collector = new SourceCollector();
            boolean moreAvailable = true;
            while (running && moreAvailable) {
                moreAvailable = source.poll(collector);
                emitPeriodicWatermark();
                // Ship whatever this poll produced rather than holding a partial buffer until
                // the next one fills it. A source that polls every 200ms and batches by size
                // alone would add the poll interval to every record's latency.
                output.flush();
            }

            if (!moreAvailable) {
                // The source is permanently exhausted. MAX closes every window still open
                // downstream, so a bounded replay finishes cleanly instead of leaving its last
                // sessions unemitted. An unbounded source never reaches this.
                log.info("source task {} reached end of stream", taskId);
                output.broadcast(Watermark.MAX);
                output.flush();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("source task {} interrupted, stopping", taskId);
        } catch (TaskCancelledException e) {
            log.info("source task {} cancelled: {}", taskId, e.getMessage());
        } catch (Exception e) {
            log.error("source task {} failed", taskId, e);
            failureListener.accept(taskId, e);
        } finally {
            closeQuietly();
            log.info("source task {} stopped after {} records", taskId, recordsOut.count());
        }
    }

    /**
     * Asks this task to stop once the source returns from its current poll.
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
     * Registers what to do when this task fails.
     *
     * @param listener called with the task id and the failure
     */
    public void onFailure(BiConsumer<String, Throwable> listener) {
        this.failureListener = listener;
    }

    private void closeQuietly() {
        try {
            source.close();
        } catch (Exception e) {
            log.warn("source task {} failed to close its source cleanly", taskId, e);
        }
        output.close();
    }

    /**
     * Stamps each emitted value with its event time on the way past.
     *
     * <p>This is the only place in the engine where event time is assigned. A source with no
     * assigner configured emits records stamped {@link Long#MIN_VALUE}, which is honest: the job
     * has not been told when anything happened, and any window downstream will say so by never
     * firing.
     */
    private final class SourceCollector implements Collector<Object> {

        @Override
        public void collect(Object value) {
            collect(value, timestampAssigner
                    .map(assigner -> assigner.extractTimestamp(value))
                    .orElse(Long.MIN_VALUE));
        }

        @Override
        public void collect(Object value, long timestamp) {
            try {
                if (watermarkGenerator.isPresent()) {
                    BoundedOutOfOrdernessGenerator generator = watermarkGenerator.get();
                    boolean wasIdle = generator.isIdle();
                    generator.onEvent(timestamp);
                    if (wasIdle) {
                        // A record alone cannot revive a downstream channel: its watermark
                        // state is tracked independently of the data path.
                        output.broadcast(Watermark.active());
                    }
                }
                output.emit(new StreamRecord<>(value, timestamp));
                recordsOut.increment();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new TaskCancelledException("cancelled while emitting from source");
            }
        }
    }

    private void emitPeriodicWatermark() throws InterruptedException {
        if (watermarkGenerator.isEmpty()) {
            return;
        }
        BoundedOutOfOrdernessGenerator generator = watermarkGenerator.get();
        boolean wasIdle = generator.isIdle();
        Optional<Watermark> watermark = generator.onPeriodicEmit();
        if (!wasIdle && generator.isIdle()) {
            output.broadcast(Watermark.idle());
        } else {
            watermark.ifPresent(value -> {
                try {
                    output.broadcast(value);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new TaskCancelledException("cancelled while emitting a watermark");
                }
            });
        }
    }
}
