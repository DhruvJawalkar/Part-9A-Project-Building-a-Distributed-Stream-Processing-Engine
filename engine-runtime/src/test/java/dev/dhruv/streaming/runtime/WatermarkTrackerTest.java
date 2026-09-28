package dev.dhruv.streaming.runtime;

import dev.dhruv.streaming.api.Watermark;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class WatermarkTrackerTest {

    @Test
    void usesTheMinimumAcrossActiveChannelsAndNeverMovesBackwards() {
        WatermarkTracker tracker = new WatermarkTracker(2);

        assertThat(tracker.onWatermark(0, new Watermark(100L))).isEmpty();
        assertThat(tracker.onWatermark(1, new Watermark(80L))).contains(new Watermark(80L));
        assertThat(tracker.onWatermark(0, new Watermark(120L))).isEmpty();
        assertThat(tracker.onWatermark(1, new Watermark(110L))).contains(new Watermark(110L));
        assertThat(tracker.onWatermark(1, new Watermark(90L))).isEmpty();
        assertThat(tracker.currentWatermark()).isEqualTo(110L);
    }

    @Test
    void anIdleChannelStopsStallingTheActiveInput() {
        WatermarkTracker tracker = new WatermarkTracker(2);
        tracker.onWatermark(0, new Watermark(1_000L));

        // This is the idleness regression proof: channel 1 has never emitted a watermark and
        // would hold the minimum at Long.MIN_VALUE forever if it were not made idle.
        assertThat(tracker.markIdle(1)).contains(new Watermark(1_000L));
        assertThat(tracker.currentWatermark()).isEqualTo(1_000L);

        tracker.onWatermark(1, new Watermark(1_100L));
        assertThat(tracker.isIdle(1)).isFalse();
        assertThat(tracker.onWatermark(0, new Watermark(1_200L)))
                .contains(new Watermark(1_100L));
    }

    @Test
    void allIdleInputsDoNotInventProgress() {
        WatermarkTracker tracker = new WatermarkTracker(2);

        assertThat(tracker.markIdle(0)).isEmpty();
        assertThat(tracker.markIdle(1)).isEmpty();
        assertThat(tracker.currentWatermark()).isEqualTo(Long.MIN_VALUE);
    }

    @Test
    void anEndedChannelIsTerminalWithoutMakingAnIdleChannelLookActive() {
        WatermarkTracker tracker = new WatermarkTracker(2);
        tracker.onWatermark(0, new Watermark(100L));
        tracker.markIdle(1);

        assertThat(tracker.allInputsIdle()).isFalse();
        assertThat(tracker.endOfInput(0)).isEmpty();
        assertThat(tracker.allInputsIdle()).isTrue();

        // The idle input can still revive after its peer has ended, and is then the only
        // remaining input that governs progress.
        assertThat(tracker.onWatermark(1, new Watermark(200L))).contains(new Watermark(200L));
    }
}
