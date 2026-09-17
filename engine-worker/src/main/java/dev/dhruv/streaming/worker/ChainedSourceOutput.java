package dev.dhruv.streaming.worker;

import dev.dhruv.streaming.api.Collector;
import dev.dhruv.streaming.api.Operator;
import dev.dhruv.streaming.api.OperatorContext;
import dev.dhruv.streaming.api.StreamElement;
import dev.dhruv.streaming.api.StreamRecord;
import dev.dhruv.streaming.runtime.RuntimeOperatorContext;
import dev.dhruv.streaming.runtime.metrics.TaskMetricGroup;
import dev.dhruv.streaming.runtime.transport.Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link Output} that runs a record through chained operators before sending it on.
 *
 * <p>This is how a source gets chained. A source task emits through its {@code Output}, so
 * putting the fused operators <em>inside</em> the output means a record read from Kafka is
 * filtered and mapped by the same thread that read it, on the same stack, with no queue and no
 * serialization anywhere between.
 *
 * <p>In the LMS job that is the difference between eight threads and four: {@code clicks} and
 * {@code drop-bots} become one task, and every bot event is discarded before it has cost a single
 * byte of transport.
 */
final class ChainedSourceOutput implements Output {

    private static final Logger log = LoggerFactory.getLogger(ChainedSourceOutput.class);

    private final Operator<Object, Object> chained;
    private final Output downstream;
    private final TaskMetricGroup metrics;
    private final Collector<Object> collector;

    /**
     * Event time of the record currently passing through. Safe as a field because a source task
     * and everything chained into it run on exactly one thread.
     */
    private long currentTimestamp;

    private boolean opened;

    @SuppressWarnings("unchecked")
    ChainedSourceOutput(Operator<?, ?> chained, Output downstream, TaskMetricGroup metrics) {
        this.chained = (Operator<Object, Object>) chained;
        this.downstream = downstream;
        this.metrics = metrics;
        this.collector = new DownstreamCollector();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void emit(StreamRecord<?> record) throws InterruptedException {
        openOnce();
        currentTimestamp = record.timestamp();
        try {
            chained.processElement((StreamRecord<Object>) record, collector);
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("a chained operator failed", e);
        }
    }

    @Override
    public void broadcast(StreamElement element) throws InterruptedException {
        // Control elements pass through the chain rather than being processed by it. A watermark
        // does reach the chained operators -- Phase 3 wires that up -- but it is forwarded
        // regardless, because downstream tasks are waiting for it.
        downstream.broadcast(element);
    }

    @Override
    public void flush() throws InterruptedException {
        downstream.flush();
    }

    @Override
    public void close() {
        try {
            chained.close();
        } catch (Exception e) {
            log.warn("a chained operator failed to close cleanly", e);
        }
        downstream.close();
    }

    /**
     * Opens the chained operators on first use.
     *
     * <p>Lazily, because a source task opens its source rather than its output, and the chained
     * operators need an {@link OperatorContext} that only exists once the task is running.
     */
    private void openOnce() {
        if (opened) {
            return;
        }
        opened = true;
        try {
            chained.open(new RuntimeOperatorContext(metrics));
        } catch (Exception e) {
            throw new IllegalStateException("a chained operator failed to open", e);
        }
    }

    /** What the chained operators emit into: the task's real output. */
    private final class DownstreamCollector implements Collector<Object> {

        @Override
        public void collect(Object value) {
            collect(value, currentTimestamp);
        }

        @Override
        public void collect(Object value, long timestamp) {
            try {
                downstream.emit(new StreamRecord<>(value, timestamp));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("cancelled while emitting from a chained source");
            }
        }
    }
}
