package dev.dhruv.streaming.runtime;

import dev.dhruv.streaming.api.Watermark;

import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Generates source watermarks by subtracting a bounded amount of tolerated disorder from the
 * greatest event timestamp observed so far.
 *
 * <p>Wall-clock time is used only to decide whether this source has become idle. Event time
 * still determines the watermark, which keeps a replay deterministic regardless of its speed.
 * An idle source emits no further watermarks; its downstream task must mark that input idle so
 * it no longer holds back the minimum watermark of active inputs.
 */
public final class BoundedOutOfOrdernessGenerator {

    private final long outOfOrdernessMillis;
    private final long idleTimeoutMillis;
    private final LongSupplier clockMillis;

    private long maxEventTime = Long.MIN_VALUE;
    private boolean observedEvent;
    private long lastActivityWallClock;
    private boolean idle;

    /**
     * Creates a generator using the system wall clock for idleness detection.
     *
     * @param outOfOrdernessMillis largest tolerated gap between arrival and event-time order
     * @param idleTimeoutMillis wall-clock silence after which this source is idle. Use
     *                          {@link Long#MAX_VALUE} to effectively disable detection.
     */
    public BoundedOutOfOrdernessGenerator(long outOfOrdernessMillis, long idleTimeoutMillis) {
        this(outOfOrdernessMillis, idleTimeoutMillis, System::currentTimeMillis);
    }

    /**
     * Creates a generator with an explicit clock, useful for deterministic tests and embeddings
     * which already own a wall clock.
     */
    public BoundedOutOfOrdernessGenerator(long outOfOrdernessMillis, long idleTimeoutMillis,
                                           LongSupplier clockMillis) {
        if (outOfOrdernessMillis < 0) {
            throw new IllegalArgumentException("outOfOrdernessMillis must be non-negative");
        }
        if (idleTimeoutMillis < 0) {
            throw new IllegalArgumentException("idleTimeoutMillis must be non-negative");
        }
        this.outOfOrdernessMillis = outOfOrdernessMillis;
        this.idleTimeoutMillis = idleTimeoutMillis;
        this.clockMillis = Objects.requireNonNull(clockMillis, "clockMillis");
        this.lastActivityWallClock = clockMillis.getAsLong();
    }

    /** Records an event and makes a previously idle source active again. */
    public void onEvent(long eventTime) {
        maxEventTime = Math.max(maxEventTime, eventTime);
        observedEvent = true;
        lastActivityWallClock = clockMillis.getAsLong();
        idle = false;
    }

    /**
     * Performs the periodic source-side watermark check.
     *
     * @return a new watermark when the source is active and has observed an event, otherwise
     *         empty. Empty on idleness is intentional: it is the signal for the runtime to
     *         exclude this input from the downstream minimum.
     */
    public Optional<Watermark> onPeriodicEmit() {
        if (clockMillis.getAsLong() - lastActivityWallClock > idleTimeoutMillis) {
            idle = true;
            return Optional.empty();
        }
        if (!observedEvent) {
            return Optional.empty();
        }
        return Optional.of(new Watermark(maxEventTime - outOfOrdernessMillis));
    }

    /** Returns whether the most recent periodic check found this source idle. */
    public boolean isIdle() {
        return idle;
    }

    /**
     * Captures the event-time progress needed to resume a source after a checkpoint.
     *
     * <p>A source offset by itself is not enough: a restored session operator may have timers
     * beyond every replayed record, so its restored source must retain the greatest event time
     * already seen in order to advance the same watermark again.
     */
    public Snapshot snapshot() {
        return new Snapshot(maxEventTime, observedEvent, lastActivityWallClock, idle);
    }

    /** Restores event-time and idleness progress from {@link #snapshot()}. */
    public void restore(Snapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        maxEventTime = snapshot.maxEventTime();
        observedEvent = snapshot.observedEvent();
        lastActivityWallClock = snapshot.lastActivityWallClock();
        idle = snapshot.idle();
    }

    /** Serializable source watermark progress saved with a source checkpoint. */
    public record Snapshot(long maxEventTime, boolean observedEvent, long lastActivityWallClock,
                           boolean idle) {
    }
}
