package dev.dhruv.streaming.api;

/**
 * A marker injected into the stream at the sources that logically splits it into a
 * pre-checkpoint half and a post-checkpoint half.
 *
 * <p>This is the Chandy-Lamport marker. Its meaning to a task is: "everything you received
 * before this barrier belongs in checkpoint {@code checkpointId}; everything after it does
 * not." Because barriers travel in band with records and channels preserve order, a task can
 * snapshot exactly the right state without any global pause -- it just has to wait until the
 * barrier has arrived on <em>every</em> input channel first, which is what alignment is.
 *
 * @param checkpointId     monotonically increasing id assigned by the checkpoint coordinator
 * @param triggerTimestamp wall-clock time the coordinator started this checkpoint, used only
 *                         for reporting checkpoint duration
 */
public record CheckpointBarrier(long checkpointId, long triggerTimestamp)
        implements StreamElement {
}
