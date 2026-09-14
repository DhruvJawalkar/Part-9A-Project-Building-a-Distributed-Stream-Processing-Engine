package dev.dhruv.streaming.api;

import dev.dhruv.streaming.api.metrics.MetricGroup;
import dev.dhruv.streaming.api.state.ListState;
import dev.dhruv.streaming.api.state.ValueState;

/**
 * Everything an operator can reach outside of its own inputs and outputs: keyed state, event
 * timers, the event-time clock, and metrics.
 *
 * <p>Handed to the operator once in {@link Operator#open}. Implementations that need any of it
 * should keep the context in a field, and should acquire their state handles in {@code open}
 * rather than per record. The handles are cheap to hold and re-fetching one on every call adds
 * noise to the one method a reader most wants to follow.
 *
 * <p>Every state and timer method is scoped to the current key, which the runtime sets before
 * each call. The operator never names a key.
 */
public interface OperatorContext {

    /**
     * Returns the named value state for this operator.
     *
     * @param name stable name, part of the checkpoint format
     * @param type the stored type
     * @param <T>  the stored type
     * @return value state scoped to the current key
     */
    <T> ValueState<T> getValueState(String name, Class<T> type);

    /**
     * Returns the named list state for this operator.
     *
     * @param name stable name, part of the checkpoint format
     * @param type the element type
     * @param <T>  the element type
     * @return list state scoped to the current key
     */
    <T> ListState<T> getListState(String name, Class<T> type);

    /**
     * Asks to be called back through {@link KeyedOperator#onEventTimer} when the event-time
     * clock reaches {@code timestamp}. Scoped to the current key.
     *
     * <p>Registering a timer is a request, not a reservation. There is no cancel: an operator
     * that changes its mind registers a new timer and must recognise the stale one when it
     * fires. Making cancellation explicit would be a bigger API; making the callback check a
     * condition it already has in state is smaller and harder to get subtly wrong.
     *
     * <p>A timer whose timestamp is already behind the current watermark fires on the next
     * clock advance rather than being dropped.
     *
     * @param timestamp event time to fire at, in milliseconds since the epoch
     */
    void registerEventTimer(long timestamp);

    /**
     * Returns this operator's current event-time clock: the minimum watermark across its
     * non-idle input channels.
     *
     * <p>{@link Long#MIN_VALUE} before any watermark has arrived, which is the honest answer.
     * At that point the operator knows nothing about how complete its input is.
     *
     * @return the current watermark
     */
    long currentWatermark();

    /**
     * Returns the metric group for this operator instance, already scoped to its subtask.
     *
     * @return this operator's metrics
     */
    MetricGroup metrics();
}
