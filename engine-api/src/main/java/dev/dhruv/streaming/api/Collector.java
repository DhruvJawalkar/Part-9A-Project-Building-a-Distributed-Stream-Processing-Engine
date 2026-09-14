package dev.dhruv.streaming.api;

/**
 * The channel an operator emits through. Handed to the operator on every call rather than
 * stored, so it is always obvious in the source where output can be produced.
 *
 * <p>An operator may call {@code collect} zero, one, or many times per input record. That
 * freedom is why {@link Operator#processElement} returns {@code void} instead of a value:
 * filters emit nothing, joins emit several.
 *
 * @param <T> the type this collector accepts
 */
public interface Collector<T> {

    /**
     * Emits a value carrying the event-time timestamp of the record currently being
     * processed. This is what almost every operator wants: a transformed record happened
     * when its input happened.
     *
     * @param value the value to emit
     */
    void collect(T value);

    /**
     * Emits a value at an explicitly chosen event time.
     *
     * <p>Needed by operators whose output is not "about" the record that triggered it -- a
     * session row, for instance, is emitted when a timer fires and should be stamped with the
     * session's end, not with whatever record happened to be in flight.
     *
     * @param value     the value to emit
     * @param timestamp event time to stamp it with, in milliseconds since the epoch
     */
    void collect(T value, long timestamp);
}
