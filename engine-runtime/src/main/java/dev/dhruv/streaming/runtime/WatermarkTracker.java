package dev.dhruv.streaming.runtime;

import dev.dhruv.streaming.api.Watermark;

import java.util.Arrays;
import java.util.Optional;

/**
 * Maintains one watermark per input channel and exposes their minimum as a task's event-time
 * clock. An idle channel is deliberately omitted from that minimum: waiting for a partition
 * that has no more records would otherwise stall every active partition forever.
 */
public final class WatermarkTracker {

    private final long[] channelWatermarks;
    private final boolean[] idleChannels;
    private final boolean[] endedChannels;
    private long currentWatermark = Long.MIN_VALUE;

    /** Creates a tracker for a task with {@code channelCount} input channels. */
    public WatermarkTracker(int channelCount) {
        if (channelCount < 1) {
            throw new IllegalArgumentException("channelCount must be at least one");
        }
        this.channelWatermarks = new long[channelCount];
        Arrays.fill(channelWatermarks, Long.MIN_VALUE);
        this.idleChannels = new boolean[channelCount];
        this.endedChannels = new boolean[channelCount];
    }

    /**
     * Records a channel watermark. A channel carrying a watermark is active again.
     *
     * @return the task watermark if this update advanced it, otherwise empty
     */
    public Optional<Watermark> onWatermark(int channelIndex, Watermark watermark) {
        checkChannel(channelIndex);
        if (watermark.isIdle()) {
            return markIdle(channelIndex);
        }
        idleChannels[channelIndex] = false;
        channelWatermarks[channelIndex] = Math.max(channelWatermarks[channelIndex],
                watermark.timestamp());
        return advanceIfPossible();
    }

    /**
     * Excludes a silent channel from the minimum until it becomes active again.
     *
     * @return the task watermark if removing this channel advanced it, otherwise empty
     */
    public Optional<Watermark> markIdle(int channelIndex) {
        checkChannel(channelIndex);
        if (endedChannels[channelIndex]) {
            return Optional.empty();
        }
        idleChannels[channelIndex] = true;
        return advanceIfPossible();
    }

    /** Marks a channel active without changing the latest watermark it reported. */
    public Optional<Watermark> markActive(int channelIndex) {
        checkChannel(channelIndex);
        if (endedChannels[channelIndex]) {
            return Optional.empty();
        }
        idleChannels[channelIndex] = false;
        return advanceIfPossible();
    }

    /**
     * Records that one input is permanently finished without declaring the whole task finished.
     * Its terminal watermark is effectively infinity in the input minimum, so remaining active
     * channels can keep advancing event time. Unlike an idle channel, an ended channel is not
     * revivable and therefore stays out of aggregate idleness transitions.
     *
     * @return the task watermark if the remaining inputs now permit progress
     */
    public Optional<Watermark> endOfInput(int channelIndex) {
        checkChannel(channelIndex);
        endedChannels[channelIndex] = true;
        idleChannels[channelIndex] = false;
        channelWatermarks[channelIndex] = Long.MAX_VALUE;
        return advanceIfPossible();
    }

    /** Returns the clock most recently advanced by this tracker. */
    public long currentWatermark() {
        return currentWatermark;
    }

    /** Returns whether a particular input is currently excluded from the minimum. */
    public boolean isIdle(int channelIndex) {
        checkChannel(channelIndex);
        return idleChannels[channelIndex];
    }

    /** Returns whether every input is currently excluded from the watermark minimum. */
    public boolean allInputsIdle() {
        for (int channel = 0; channel < idleChannels.length; channel++) {
            if (!endedChannels[channel] && !idleChannels[channel]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Advances the task clock to the end of a bounded input only after its task has established
     * that <em>every</em> channel ended. Keeping that decision outside the ordinary per-channel
     * update avoids an idle channel making a premature MAX watermark look safe.
     */
    public Optional<Watermark> endOfAllInputs() {
        Arrays.fill(channelWatermarks, Long.MAX_VALUE);
        Arrays.fill(idleChannels, false);
        Arrays.fill(endedChannels, true);
        if (currentWatermark == Long.MAX_VALUE) {
            return Optional.empty();
        }
        currentWatermark = Long.MAX_VALUE;
        return Optional.of(Watermark.MAX);
    }

    private Optional<Watermark> advanceIfPossible() {
        long minimum = Long.MAX_VALUE;
        for (int channel = 0; channel < channelWatermarks.length; channel++) {
            if (!idleChannels[channel]) {
                minimum = Math.min(minimum, channelWatermarks[channel]);
            }
        }
        if (minimum == Long.MAX_VALUE || minimum <= currentWatermark) {
            return Optional.empty();
        }
        currentWatermark = minimum;
        return Optional.of(new Watermark(minimum));
    }

    private void checkChannel(int channelIndex) {
        if (channelIndex < 0 || channelIndex >= channelWatermarks.length) {
            throw new IndexOutOfBoundsException("unknown input channel " + channelIndex);
        }
    }
}
