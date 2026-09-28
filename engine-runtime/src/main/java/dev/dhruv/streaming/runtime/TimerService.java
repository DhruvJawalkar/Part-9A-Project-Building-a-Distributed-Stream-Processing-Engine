package dev.dhruv.streaming.runtime;

import dev.dhruv.streaming.api.state.StateBackend;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
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

    /**
     * Captures the event-time clock and all not-yet-fired keyed timers for a task checkpoint.
     *
     * <p>Timer state deliberately remains a small task-level snapshot rather than masquerading
     * as user keyed state in {@link StateBackend}. A checkpoint containing only an operator's
     * accumulator could restore an open session but never fire it; this value is the other half
     * of a keyed operator's recoverable state. The task checkpoint envelope owns serialization
     * and storage of this record alongside its {@code StateHandle}.
     */
    public TimerSnapshot snapshot() {
        List<TimerRegistration> registrations = new ArrayList<>();
        timersByTimestamp.forEach((timestamp, keys) ->
                keys.forEach(key -> registrations.add(new TimerRegistration(timestamp, key))));
        return new TimerSnapshot(currentWatermark, List.copyOf(registrations));
    }

    /**
     * Replaces timer state from a task checkpoint. This is a whole-state replacement, just like
     * {@link StateBackend#restore(dev.dhruv.streaming.api.state.StateHandle)}: merge semantics
     * would retain timers registered after the checkpoint and fire them twice after recovery.
     */
    public void restore(TimerSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        timersByTimestamp.clear();
        currentWatermark = snapshot.currentWatermark();
        for (TimerRegistration registration : snapshot.registrations()) {
            registerEventTimeTimer(registration.key(), registration.timestamp());
        }
    }

    /** Serializable task-level timer snapshot, intended for a checkpoint envelope. */
    public record TimerSnapshot(long currentWatermark, List<TimerRegistration> registrations)
            implements Serializable {
        public TimerSnapshot {
            registrations = List.copyOf(registrations);
        }
    }

    /** One pending timer; the key is restored before the callback runs. */
    public record TimerRegistration(long timestamp, Object key) implements Serializable {
        public TimerRegistration {
            Objects.requireNonNull(key, "key");
        }
    }

    /** Callback used by the task loop to restore key context before invoking a keyed operator. */
    @FunctionalInterface
    public interface TimerCallback {
        void onEventTime(long timestamp, Object key) throws Exception;
    }
}
