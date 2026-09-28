package dev.dhruv.streaming.runtime;

import dev.dhruv.streaming.api.CheckpointBarrier;
import dev.dhruv.streaming.runtime.transport.InputGate;

import java.util.BitSet;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Aligns one in-band checkpoint barrier from every input channel of a task.
 *
 * <p>This is Chandy-Lamport in its deliberately visible form. A barrier says that all records
 * before it on <em>that</em> channel belong to a checkpoint. Once a channel delivers its
 * barrier, this aligner blocks it in the {@link InputGate}; records arriving afterwards wait
 * there while the task keeps consuming the other, still pre-checkpoint channels. Once every
 * channel has delivered the same barrier, the task has a consistent cut to snapshot.
 *
 * <p>The caller must preserve the final order: snapshot, forward the returned barrier,
 * acknowledge it to the coordinator, then call {@link #completeAlignment(long)}. Keeping the
 * channels blocked until that explicit final call means post-barrier records can never slip
 * into the snapshot or overtake the downstream barrier.
 */
public final class BarrierAligner {

    private final InputGate inputGate;
    private final LongSupplier nanoClock;
    private final BitSet barrierChannels;

    private CheckpointBarrier aligningBarrier;
    private long alignmentStartedNanos;
    private boolean snapshotReady;

    /** Creates an aligner that measures elapsed time with the system monotonic clock. */
    public BarrierAligner(InputGate inputGate) {
        this(inputGate, System::nanoTime);
    }

    /**
     * Creates an aligner with an injectable monotonic clock. The overload exists so the timing
     * shown in the teaching tests is deterministic; production callers should use the public
     * single-argument constructor.
     */
    BarrierAligner(InputGate inputGate, LongSupplier nanoClock) {
        this.inputGate = Objects.requireNonNull(inputGate, "inputGate");
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
        this.barrierChannels = new BitSet(inputGate.channelCount());
    }

    /**
     * Records a barrier arriving on one input channel.
     *
     * @return the aligned checkpoint only when every channel has delivered this same barrier;
     *         empty while other channels are still catching up
     * @throws IllegalStateException if a channel repeats a barrier, a different checkpoint
     *                               overtakes the alignment, or a new barrier arrives before
     *                               the previous one has been completed
     */
    public synchronized Optional<AlignedCheckpoint> onBarrier(CheckpointBarrier barrier,
                                                               int channelIndex) {
        Objects.requireNonNull(barrier, "barrier");
        validateChannel(channelIndex);

        // There is no distributed cut to wait for with a single input. More importantly, do not
        // block the only input while the task snapshots: that would only create avoidable
        // backpressure and obscures the simple case this fast path is meant to teach.
        if (inputGate.channelCount() == 1) {
            return Optional.of(new AlignedCheckpoint(barrier, 0));
        }

        if (snapshotReady) {
            throw new IllegalStateException("checkpoint " + aligningBarrier.checkpointId()
                    + " is aligned but has not been completed yet");
        }
        if (aligningBarrier == null) {
            aligningBarrier = barrier;
            alignmentStartedNanos = nanoClock.getAsLong();
        } else if (aligningBarrier.checkpointId() != barrier.checkpointId()) {
            throw new IllegalStateException("received checkpoint " + barrier.checkpointId()
                    + " on channel " + channelIndex + " while aligning checkpoint "
                    + aligningBarrier.checkpointId());
        }
        if (barrierChannels.get(channelIndex)) {
            throw new IllegalStateException("channel " + channelIndex + " delivered checkpoint "
                    + barrier.checkpointId() + " more than once");
        }

        barrierChannels.set(channelIndex);
        inputGate.blockChannel(channelIndex);
        if (barrierChannels.cardinality() != inputGate.channelCount()) {
            return Optional.empty();
        }

        snapshotReady = true;
        long elapsedNanos = Math.max(0, nanoClock.getAsLong() - alignmentStartedNanos);
        return Optional.of(new AlignedCheckpoint(aligningBarrier, elapsedNanos / 1_000_000));
    }

    /**
     * Releases buffered post-barrier records after the task has snapshotted, forwarded and
     * acknowledged the checkpoint.
     */
    public synchronized void completeAlignment(long checkpointId) {
        if (inputGate.channelCount() == 1) {
            return;
        }
        // The coordinator can time out while the task is publishing/acknowledging its snapshot.
        // Its abort command releases the channels from another control-plane thread. Completion
        // of that same attempt is then a harmless no-op, not a reason to fail the task.
        if (aligningBarrier == null) {
            return;
        }
        if (!snapshotReady || aligningBarrier == null
                || aligningBarrier.checkpointId() != checkpointId) {
            throw new IllegalStateException("checkpoint " + checkpointId
                    + " is not ready to complete");
        }
        resetAndUnblock();
    }

    /**
     * Abandons an in-flight checkpoint, for example after the coordinator times it out.
     * Buffered post-barrier records are still ordinary records, so they are released to run.
     */
    public synchronized void abortAlignment(long checkpointId) {
        if (inputGate.channelCount() == 1) {
            return;
        }
        if (aligningBarrier == null || aligningBarrier.checkpointId() != checkpointId) {
            return;
        }
        resetAndUnblock();
    }

    /** Returns how many inputs have delivered the currently aligning barrier. */
    public synchronized int arrivedBarrierCount() {
        return barrierChannels.cardinality();
    }

    /** Returns whether all input barriers have arrived and the task may take its snapshot. */
    public synchronized boolean isSnapshotReady() {
        return snapshotReady;
    }

    private void resetAndUnblock() {
        inputGate.unblockAllChannels();
        barrierChannels.clear();
        aligningBarrier = null;
        alignmentStartedNanos = 0;
        snapshotReady = false;
    }

    private void validateChannel(int channelIndex) {
        if (channelIndex < 0 || channelIndex >= inputGate.channelCount()) {
            throw new IllegalArgumentException("channel " + channelIndex + " is outside 0.."
                    + (inputGate.channelCount() - 1));
        }
    }

    /**
     * A fully aligned checkpoint and the time spent waiting for the slower input channels.
     * The initial barrier is retained because its trigger timestamp travels downstream intact.
     */
    public record AlignedCheckpoint(CheckpointBarrier barrier, long alignmentMillis) {
    }
}
