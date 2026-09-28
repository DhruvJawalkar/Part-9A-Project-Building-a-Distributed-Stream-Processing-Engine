package dev.dhruv.streaming.runtime;

import dev.dhruv.streaming.api.OperatorContext;
import dev.dhruv.streaming.api.metrics.MetricGroup;
import dev.dhruv.streaming.api.state.StateBackend;
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
public final class RuntimeOperatorContext implements OperatorContext {

    private final MetricGroup metrics;
    private final StateBackend stateBackend;
    private final TimerService timerService;
    private final WatermarkTracker watermarkTracker;
    private Object currentKey;

    public RuntimeOperatorContext(MetricGroup metrics) {
        this(metrics, null, null, null);
    }

    RuntimeOperatorContext(MetricGroup metrics,
                           StateBackend stateBackend,
                           TimerService timerService,
                           WatermarkTracker watermarkTracker) {
        this.metrics = metrics;
        this.stateBackend = stateBackend;
        this.timerService = timerService;
        this.watermarkTracker = watermarkTracker;
    }

    @Override
    public <T> ValueState<T> getValueState(String name, Class<T> type) {
        requireKeyedRuntime("value state '" + name + "'");
        return stateBackend.valueState(name, type);
    }

    @Override
    public <T> ListState<T> getListState(String name, Class<T> type) {
        requireKeyedRuntime("list state '" + name + "'");
        return stateBackend.listState(name, type);
    }

    @Override
    public void registerEventTimer(long timestamp) {
        requireKeyedRuntime("event timers");
        if (currentKey == null) {
            throw new IllegalStateException("an event timer was registered before the runtime"
                    + " established a current key");
        }
        timerService.registerEventTimeTimer(currentKey, timestamp);
    }

    @Override
    public long currentWatermark() {
        return watermarkTracker == null ? Long.MIN_VALUE : watermarkTracker.currentWatermark();
    }

    @Override
    public MetricGroup metrics() {
        return metrics;
    }

    void setCurrentKey(Object key) {
        this.currentKey = key;
    }

    private void requireKeyedRuntime(String what) {
        if (stateBackend != null && timerService != null) {
            return;
        }
        throw new UnsupportedOperationException(
                what + " is not available until Phase 3, which adds the state backend,"
                        + " the timer service and watermark propagation together");
    }
}
