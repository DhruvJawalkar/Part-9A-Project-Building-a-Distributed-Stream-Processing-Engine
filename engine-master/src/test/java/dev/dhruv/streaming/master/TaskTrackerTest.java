package dev.dhruv.streaming.master;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TaskTrackerTest {

    @Test
    void firstHeartbeatSeedsLivenessAfterAMasterRestart() {
        try (TaskTracker tracker = new TaskTracker(ignored -> { })) {
            tracker.heartbeatReceived("surviving-worker");

            assertThat(tracker.isAlive("surviving-worker")).isTrue();
            assertThat(tracker.aliveWorkers()).containsExactly("surviving-worker");
        }
    }
}
