package dev.dhruv.streaming.api;

import java.io.Serializable;

/**
 * The unit of user logic in a job: one input type in, one output type out.
 *
 * <p>Instances are used by exactly one task thread. The runtime gives every task a single
 * thread and an inbound queue, so operator implementations never need to be thread-safe and
 * never need to synchronise their own fields. This is a guarantee worth relying on -- it is
 * the same one production engines provide, and giving it up is how state handling gets hard.
 *
 * <p>Lifecycle: {@link #open} once, then any number of {@link #processElement} and
 * {@link #onWatermark} calls interleaved in arrival order, then {@link #close} once.
 *
 * <p>Operators that need keyed state or event-time timers implement {@link KeyedOperator}
 * instead.
 *
 * <h2>Why this is {@link Serializable}</h2>
 *
 * <p>The job is written in one process and runs in another. From Phase 2 on, the object you
 * construct in {@code main()} is serialized, sent to a worker over the network, and
 * deserialized there before it ever sees a record. Two consequences follow, and both are
 * ordinary sources of confusion worth naming up front:
 *
 * <ul>
 *   <li>Every field an operator holds must itself be serializable. A field holding a database
 *       connection, a Kafka client or a file handle will fail to ship.</li>
 *   <li>That is exactly what {@link #open} is for. Configuration travels in fields;
 *       <em>resources</em> are acquired in {@code open}, after the object has arrived on the
 *       worker that will actually use it.</li>
 * </ul>
 *
 * @param <IN>  input record type
 * @param <OUT> output record type
 */
public interface Operator<IN, OUT> extends Serializable {

    /**
     * Called once per input record.
     *
     * @param record the record and its event time
     * @param out    where to emit results, zero or more times
     * @throws Exception any failure; the runtime fails the task and reports it to the master
     */
    void processElement(StreamRecord<IN> record, Collector<OUT> out) throws Exception;

    /**
     * Called when this operator's event-time clock advances -- that is, when the minimum
     * watermark across its non-idle input channels moves forward.
     *
     * <p>This is the hook for work that is driven by the passage of event time rather than by
     * the arrival of a record: evicting buffered state that can no longer match, for example.
     * The default does nothing.
     *
     * @param watermark the new event-time clock value
     * @param out       where to emit any results triggered by time advancing
     * @throws Exception any failure; the runtime fails the task
     */
    default void onWatermark(long watermark, Collector<OUT> out) throws Exception {
    }

    /**
     * Called once before the first record, with the context through which state, timers and
     * metrics are obtained. Implementations that need any of those should keep the context in
     * a field.
     *
     * @param ctx this operator instance's context
     * @throws Exception any failure; the task fails to start
     */
    default void open(OperatorContext ctx) throws Exception {
    }

    /**
     * Called once after the last record, or when the task is cancelled. Release resources
     * here. Not a place to emit -- by the time this runs the output is already gone.
     *
     * @throws Exception any failure; logged, but the task still stops
     */
    default void close() throws Exception {
    }
}
