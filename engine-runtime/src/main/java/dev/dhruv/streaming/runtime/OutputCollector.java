package dev.dhruv.streaming.runtime;

import dev.dhruv.streaming.api.Collector;
import dev.dhruv.streaming.runtime.transport.Output;
import dev.dhruv.streaming.api.StreamRecord;

/**
 * The {@link Collector} handed to an operator, wrapping the task's {@link Output}.
 *
 * <p>Its one interesting job is carrying the event time of the record being processed, so that
 * {@code out.collect(value)} can stamp its output without the operator having to pass a
 * timestamp it never asked to think about. A map that transforms a record does not change when
 * the underlying event happened, and making that the default is what keeps event time from
 * quietly decaying into processing time one operator at a time.
 *
 * @param <T> the type this collector accepts
 */
final class OutputCollector<T> implements Collector<T> {

    private final Output output;

    /**
     * Event time of the record currently being processed. Set by the task immediately before it
     * calls into the operator.
     */
    private long currentTimestamp;

    OutputCollector(Output output) {
        this.output = output;
    }

    @Override
    public void collect(T value) {
        collect(value, currentTimestamp);
    }

    @Override
    public void collect(T value, long timestamp) {
        try {
            output.emit(new StreamRecord<>(value, timestamp));
        } catch (InterruptedException e) {
            // The task is being cancelled. Restore the flag and unwind through the operator,
            // which is not expected to handle cancellation itself.
            Thread.currentThread().interrupt();
            throw new TaskCancelledException("cancelled while emitting downstream");
        }
    }

    void setCurrentTimestamp(long timestamp) {
        this.currentTimestamp = timestamp;
    }
}
