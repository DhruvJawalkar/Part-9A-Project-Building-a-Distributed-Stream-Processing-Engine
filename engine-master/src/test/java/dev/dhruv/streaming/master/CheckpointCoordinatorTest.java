package dev.dhruv.streaming.master;

import dev.dhruv.streaming.metadata.InMemoryMetadataStore;
import dev.dhruv.streaming.metadata.MetadataStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The coordinator's essential invariant: one durable checkpoint means every task agreed. */
class CheckpointCoordinatorTest {

    private final MetadataStore metadata = new InMemoryMetadataStore();
    private final CheckpointCoordinator coordinator = new CheckpointCoordinator(metadata,
            new CheckpointCoordinator.Config(Duration.ofDays(1), Duration.ofMillis(50)));

    @AfterEach
    void tearDown() {
        coordinator.close();
        metadata.close();
    }

    @Test
    void completesOnlyAfterEveryExpectedTaskAcknowledgesAndThenNotifiesSinks() {
        RecordingActions actions = new RecordingActions();
        Set<CheckpointCoordinator.TaskKey> tasks = Set.of(
                new CheckpointCoordinator.TaskKey("source", 0),
                new CheckpointCoordinator.TaskKey("sink", 0));
        coordinator.start("job", tasks, actions);

        long checkpoint = coordinator.trigger("job").orElseThrow();

        assertThat(actions.triggers).containsExactly(new Trigger("job", checkpoint));
        coordinator.acknowledge("job", new CheckpointCoordinator.TaskKey("source", 0), checkpoint,
                "file:///source", 3, 11);
        coordinator.acknowledge("job", new CheckpointCoordinator.TaskKey("source", 0), checkpoint,
                "ignored-duplicate", 99, 99);

        assertThat(metadata.getLatestCompletedCheckpoint("job")).isEmpty();
        assertThat(actions.completed).isEmpty();

        coordinator.acknowledge("job", new CheckpointCoordinator.TaskKey("sink", 0), checkpoint,
                "file:///sink", 7, 13);

        var persisted = metadata.getLatestCompletedCheckpoint("job").orElseThrow();
        assertThat(persisted.checkpointId()).isEqualTo(checkpoint);
        assertThat(persisted.taskStates()).containsKeys("source:0", "sink:0");
        assertThat(persisted.taskStates().get("source:0").stateHandleUri())
                .isEqualTo("file:///source");
        assertThat(actions.completed).containsExactly(new Completed("job", checkpoint));
        assertThat(coordinator.inFlightCheckpoint("job")).isEmpty();
    }

    @Test
    void rejectsAnAcknowledgementFromTaskOutsideThePhysicalPlan() {
        coordinator.start("job", Set.of(new CheckpointCoordinator.TaskKey("source", 0)),
                new RecordingActions());
        long checkpoint = coordinator.trigger("job").orElseThrow();

        assertThatThrownBy(() -> coordinator.acknowledge("job",
                new CheckpointCoordinator.TaskKey("invented", 9), checkpoint,
                "file:///wrong", 0, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unexpected task");
    }

    @Test
    void abortsAnIncompleteCheckpointWithoutPublishingIt() throws InterruptedException {
        RecordingActions actions = new RecordingActions();
        coordinator.start("job", Set.of(new CheckpointCoordinator.TaskKey("source", 0),
                new CheckpointCoordinator.TaskKey("sink", 0)), actions);
        long checkpoint = coordinator.trigger("job").orElseThrow();
        coordinator.acknowledge("job", new CheckpointCoordinator.TaskKey("source", 0), checkpoint,
                "file:///source", 0, 1);

        assertThat(actions.aborted.await(1, TimeUnit.SECONDS)).isTrue();
        assertThat(metadata.getLatestCompletedCheckpoint("job")).isEmpty();
        assertThat(actions.completed).isEmpty();
        assertThat(actions.abortIds).containsExactly(checkpoint);
    }

    private record Trigger(String jobId, long checkpointId) {
    }

    private record Completed(String jobId, long checkpointId) {
    }

    private static final class RecordingActions implements CheckpointCoordinator.Actions {
        private final List<Trigger> triggers = new java.util.ArrayList<>();
        private final List<Completed> completed = new java.util.ArrayList<>();
        private final List<Long> abortIds = new java.util.ArrayList<>();
        private final CountDownLatch aborted = new CountDownLatch(1);

        @Override
        public void triggerSources(String jobId, long checkpointId, long triggerTimestamp) {
            triggers.add(new Trigger(jobId, checkpointId));
        }

        @Override
        public void notifySinks(String jobId, long checkpointId) {
            completed.add(new Completed(jobId, checkpointId));
        }

        @Override
        public void checkpointAborted(String jobId, long checkpointId, String reason) {
            abortIds.add(checkpointId);
            aborted.countDown();
        }
    }
}
