package dev.dhruv.streaming.worker.checkpoint;

import dev.dhruv.streaming.api.state.StateHandle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Proves a checkpoint is portable, rather than merely a path on the producing worker. */
class FileSystemCheckpointStorageTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void archivesNestedTaskFilesAndMaterializesThemOnAnotherLocalPath() throws Exception {
        Path producerCheckpoint = temporaryDirectory.resolve("worker-one/checkpoint-9");
        Path nested = producerCheckpoint.resolve("keyed-state/checkpoint-9/CURRENT");
        Files.createDirectories(nested.getParent());
        Files.writeString(nested, "rocksdb-manifest");
        Path envelope = producerCheckpoint.resolve("operator-task.bin");
        Files.writeString(envelope, "relative keyed-state/checkpoint-9");

        FileSystemCheckpointStorage storage = new FileSystemCheckpointStorage(
                temporaryDirectory.resolve("durable-archives"));
        StateHandle durable = storage.publish("jobs/job/tasks/operator/checkpoint-9.zip",
                producerCheckpoint, new StateHandle(envelope.toUri(), Files.size(envelope)));

        assertThat(durable.uri().getFragment()).isEqualTo("operator-task.bin");
        assertThat(durable.uri().getPath()).endsWith("checkpoint-9.zip");
        assertThat(durable.sizeBytes()).isPositive();

        // Nothing under worker one's checkpoint directory participates in recovery.
        CheckpointArchives.deleteTree(producerCheckpoint);
        StateHandle materialized = storage.materialize(durable, temporaryDirectory.resolve("worker-two/restore"));

        Path restoredEnvelope = Path.of(materialized.uri());
        assertThat(restoredEnvelope).isRegularFile();
        assertThat(restoredEnvelope).isNotEqualTo(envelope);
        assertThat(Files.readString(restoredEnvelope)).contains("relative keyed-state/checkpoint-9");
        assertThat(restoredEnvelope.getParent().resolve("keyed-state/checkpoint-9/CURRENT"))
                .hasContent("rocksdb-manifest");
    }
}
