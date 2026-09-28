package dev.dhruv.streaming.runtime;

import dev.dhruv.streaming.api.state.ValueState;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TimerServiceTest {

    @Test
    void firesTimersInTimestampOrderAndDeduplicatesAnIdenticalRequest() throws Exception {
        TimerService timers = new TimerService();
        timers.registerEventTimeTimer("member-b", 30L);
        timers.registerEventTimeTimer("member-a", 10L);
        timers.registerEventTimeTimer("member-c", 20L);
        timers.registerEventTimeTimer("member-a", 10L);
        List<String> fired = new ArrayList<>();

        timers.advanceTo(30L, (timestamp, key) -> fired.add(key + "@" + timestamp));

        assertThat(fired).containsExactly("member-a@10", "member-c@20", "member-b@30");
        assertThat(timers.timerCount()).isZero();
    }

    @Test
    void restoresTheKeyBeforeExecutingATimerCallback() throws Exception {
        TimerService timers = new TimerService();
        InMemoryStateBackend state = new InMemoryStateBackend();
        ValueState<String> values = state.valueState("value", String.class);
        state.setCurrentKey("member-1");
        values.update("first");
        state.setCurrentKey("member-2");
        values.update("second");
        timers.registerEventTimeTimer("member-1", 10L);
        timers.registerEventTimeTimer("member-2", 20L);
        List<String> fired = new ArrayList<>();

        timers.advanceTo(20L, state, (timestamp, key) ->
                fired.add(key + ":" + values.value().orElseThrow()));

        assertThat(fired).containsExactly("member-1:first", "member-2:second");
    }

    @Test
    void timerRegisteredBehindTheClockWaitsForTheNextAdvance() throws Exception {
        TimerService timers = new TimerService();
        List<String> fired = new ArrayList<>();
        timers.advanceTo(100L, (timestamp, key) -> fired.add("unexpected"));
        timers.registerEventTimeTimer("member", 50L);

        timers.advanceTo(100L, (timestamp, key) -> fired.add("same"));
        assertThat(fired).isEmpty();

        timers.advanceTo(101L, (timestamp, key) -> fired.add(key + "@" + timestamp));
        assertThat(fired).containsExactly("member@50");
    }

    @Test
    void snapshotAndRestorePreserveTheClockAndOnlyThePendingTimers() throws Exception {
        TimerService original = new TimerService();
        original.registerEventTimeTimer("already-fired", 10L);
        original.registerEventTimeTimer("member-a", 20L);
        original.registerEventTimeTimer("member-b", 30L);
        original.advanceTo(10L, (timestamp, key) -> { });

        TimerService restored = new TimerService();
        restored.registerEventTimeTimer("discarded-on-restore", 5L);
        restored.restore(original.snapshot());
        List<String> fired = new ArrayList<>();

        // The restored clock stays at 10, so replaying watermark 10 cannot duplicate work.
        restored.advanceTo(10L, (timestamp, key) -> fired.add("unexpected"));
        restored.advanceTo(30L, (timestamp, key) -> fired.add(key + "@" + timestamp));

        assertThat(restored.currentWatermark()).isEqualTo(30L);
        assertThat(fired).containsExactly("member-a@20", "member-b@30");
        assertThat(restored.timerCount()).isZero();
    }
}
