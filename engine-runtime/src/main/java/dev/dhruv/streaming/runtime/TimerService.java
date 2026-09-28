package dev.dhruv.streaming.runtime;

import dev.dhruv.streaming.api.state.StateBackend;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Event-time timer queue for one keyed operator task.
 *
 * <p>Timers are grouped by timestamp in a {@link TreeMap}, so advancing the watermark invokes
 * callbacks in timestamp order. A key appears only once at a timestamp: registering an
 * identical timer twice is still one request to call the operator when that point in event time
 * is reached.
 */
public final class TimerService {

    private final TreeMap<Long, LinkedHashSet<Object>> timersByTimestamp = new TreeMap<>();
    private long currentWatermark = Long.MIN_VALUE;

    /** Registers an event-time timer for the key currently being processed. */
    public void registerEventTimeTimer(Object key, long timestamp) {
        Objects.requireNonNull(key, "key");
        timersByTimestamp.computeIfAbsent(timestamp, ignored -> new LinkedHashSet<>()).add(key);
    }

    /**
     * Advances event time and calls every due timer. A non-advancing watermark is harmless and
     * does not invoke callbacks, which preserves the monotonic contract of watermarks.
     *
     * @param watermark event-time boundary to advance to
     * @param callback receives the timestamp and restored key for each due timer
     * @throws Exception if callback execution fails
     */
    public void advanceTo(long watermark, TimerCallback callback) throws Exception {
        Objects.requireNonNull(callback, "callback");
        if (watermark <= currentWatermark) {
            return;
        }
        currentWatermark = watermark;

        while (!timersByTimestamp.isEmpty() && timersByTimestamp.firstKey() <= watermark) {
            Map.Entry<Long, LinkedHashSet<Object>> due = timersByTimestamp.pollFirstEntry();
            for (Object key : due.getValue()) {
                callback.onEventTime(due.getKey(), key);
            }
        }
    }

    /**
     * Advances event time while restoring the registered key before every callback. This is the
     * form used by keyed task loops, so state accessed from a timer is scoped exactly as it was
     * while processing the record which registered it.
     */
    public void advanceTo(long watermark, StateBackend stateBackend, TimerCallback callback)
            throws Exception {
        Objects.requireNonNull(stateBackend, "stateBackend");
        advanceTo(watermark, (timestamp, key) -> {
            stateBackend.setCurrentKey(key);
            callback.onEventTime(timestamp, key);
        });
    }

    /** Returns the last watermark accepted by {@link #advanceTo(long, TimerCallback)}. */
    public long currentWatermark() {
        return currentWatermark;
    }

    /** Returns the number of distinct key/timestamp timer registrations still pending. */
    public int timerCount() {
        return timersByTimestamp.values().stream().mapToInt(LinkedHashSet::size).sum();
    }

    /** Callback used by the task loop to restore key context before invoking a keyed operator. */
    @FunctionalInterface
    public interface TimerCallback {
        void onEventTime(long timestamp, Object key) throws Exception;
    }
}
