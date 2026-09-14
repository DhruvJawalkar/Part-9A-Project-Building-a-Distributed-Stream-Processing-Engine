package dev.dhruv.streaming.runtime;

import dev.dhruv.streaming.api.OperatorContext;
import dev.dhruv.streaming.api.metrics.MetricGroup;
import dev.dhruv.streaming.api.state.ListState;
import dev.dhruv.streaming.api.state.ValueState;

/**
 * The context handed to an operator in {@link dev.dhruv.streaming.api.Operator#open}.
 *
 * <p>Phase 1 supports metrics and nothing else. Keyed state and event timers arrive in Phase 3
 * together with the state backend and the timer service that make them mean something, and
 * until then asking for either fails loudly rather than returning something that silently
 * forgets what it was given.
 *
 * <p>Failing loudly is the deliberate choice. A state handle that quietly discarded every write
 * would let a job run, produce plausible output, and be wrong -- the single worst outcome for
 * an engine whose entire purpose is to be trusted with state.
 */
final class RuntimeOperatorContext implements OperatorContext {

    private final MetricGroup metrics;

    RuntimeOperatorContext(MetricGroup metrics) {
        this.metrics = metrics;
    }

    @Override
    public <T> ValueState<T> getValueState(String name, Class<T> type) {
        throw notUntilPhaseThree("value state '" + name + "'");
    }

    @Override
    public <T> ListState<T> getListState(String name, Class<T> type) {
        throw notUntilPhaseThree("list state '" + name + "'");
    }

    @Override
    public void registerEventTimer(long timestamp) {
        throw notUntilPhaseThree("event timers");
    }

    @Override
    public long currentWatermark() {
        // Phase 3 tracks this per input channel in WatermarkTracker. Until then the honest
        // answer is that the operator knows nothing about how complete its input is.
        return Long.MIN_VALUE;
    }

    @Override
    public MetricGroup metrics() {
        return metrics;
    }

    private static UnsupportedOperationException notUntilPhaseThree(String what) {
        return new UnsupportedOperationException(
                what + " is not available until Phase 3, which adds the state backend,"
                        + " the timer service and watermark propagation together");
    }
}
