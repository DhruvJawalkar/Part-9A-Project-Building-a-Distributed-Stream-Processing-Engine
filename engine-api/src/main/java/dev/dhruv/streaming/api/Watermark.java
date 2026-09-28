package dev.dhruv.streaming.api;

import java.util.Objects;

/**
 * A claim about the completeness of a stream: no element with an event time earlier than
 * {@link #timestamp()} will arrive on this channel from this point on.
 *
 * <p>It is a claim, not a guarantee. Watermarks are generated heuristically from observed
 * event times minus a tolerated out-of-orderness, so an element that violates the claim can
 * still show up -- that is precisely what a "late event" is. The engine's job is to be
 * explicit about what it does with one, not to pretend it cannot happen.
 *
 * <p>A watermark is what turns event time into a <em>clock</em>. A task's own clock is the
 * minimum watermark across its non-idle input channels: it can only be as certain as its
 * least advanced input.
 *
 * @param timestamp the completeness boundary, in milliseconds since the epoch. It is ignored
 *                  for an {@link Status#IDLE} notification.
 * @param status whether this is a usual completeness claim or an in-band idleness transition
 */
public record Watermark(long timestamp, Status status) implements StreamElement {

    /** The two states a source (or an aggregate of input channels) can report. */
    public enum Status { ACTIVE, IDLE }

    public Watermark {
        Objects.requireNonNull(status, "status");
    }

    /**
     * Keeps source compatibility with the original timestamp-only watermark API.
     * Timestamp-only watermarks are active completeness claims.
     */
    public Watermark(long timestamp) {
        this(timestamp, Status.ACTIVE);
    }

    /** A control notification that a channel must be left out of a downstream minimum. */
    public static Watermark idle() {
        return new Watermark(Long.MIN_VALUE, Status.IDLE);
    }

    /** A control notification that a previously idle channel is producing again. */
    public static Watermark active() {
        return new Watermark(Long.MIN_VALUE, Status.ACTIVE);
    }

    /** Returns whether this element is an idleness notification rather than a timestamp claim. */
    public boolean isIdle() {
        return status == Status.IDLE;
    }

    /**
     * The end-of-stream watermark. Emitted when a source has no more data, it fires every
     * remaining event-time timer and closes every open window.
     */
    public static final Watermark MAX = new Watermark(Long.MAX_VALUE, Status.ACTIVE);
}
