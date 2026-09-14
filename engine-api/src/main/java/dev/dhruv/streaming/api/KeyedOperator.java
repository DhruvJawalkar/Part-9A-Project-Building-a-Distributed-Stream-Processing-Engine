package dev.dhruv.streaming.api;

/**
 * An operator that runs against a keyed stream, and so may use keyed state and event-time
 * timers.
 *
 * <p>The runtime guarantees that all records sharing a key reach the same subtask (see
 * {@link KeyGroupAssigner}) and sets the current key before every call, so
 * {@link OperatorContext#getValueState} transparently returns the state for the key of the
 * record in hand. The operator never names the key when reading or writing state; it is
 * ambient. That is the whole trick of keyed state.
 *
 * @param <K>   the key type
 * @param <IN>  input record type
 * @param <OUT> output record type
 */
public interface KeyedOperator<K, IN, OUT> extends Operator<IN, OUT> {

    /**
     * Fired when a timer previously registered through
     * {@link OperatorContext#registerEventTimer} has been reached by the event-time clock.
     *
     * <p>Timers fire in timestamp order, and the runtime restores the key context of the timer
     * before invoking this -- so state access inside the callback is scoped to {@code key}
     * even though no record is being processed.
     *
     * <p>Note that a timer is a request to be called at a time, not a reservation that
     * survives changes of mind. An operator that registers a later timer does not thereby
     * cancel an earlier one; both will fire, and the callback is responsible for recognising
     * the stale one. See the session aggregator for what that check looks like.
     *
     * @param timestamp the timer's timestamp, which the clock has now reached
     * @param key       the key the timer was registered under, already restored as current
     * @param out       where to emit results
     * @throws Exception any failure; the runtime fails the task
     */
    void onEventTimer(long timestamp, K key, Collector<OUT> out) throws Exception;
}
