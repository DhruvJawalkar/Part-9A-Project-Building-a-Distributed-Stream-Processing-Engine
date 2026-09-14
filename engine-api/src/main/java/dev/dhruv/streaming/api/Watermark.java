package dev.dhruv.streaming.api;

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
 * @param timestamp the completeness boundary, in milliseconds since the epoch
 */
public record Watermark(long timestamp) implements StreamElement {

    /**
     * The end-of-stream watermark. Emitted when a source has no more data, it fires every
     * remaining event-time timer and closes every open window.
     */
    public static final Watermark MAX = new Watermark(Long.MAX_VALUE);
}
