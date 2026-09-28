package dev.dhruv.streaming.worker.checkpoint;

import dev.dhruv.streaming.api.state.StateHandle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies that a checkpoint uploaded by one worker can be restored on another filesystem. */
@Testcontainers
class MinioCheckpointStorageIT {

    private static final String ACCESS_KEY = "minioadmin";
    private static final String SECRET_KEY = "minioadmin";

    @Container
    private static final GenericContainer<?> MINIO = new GenericContainer<>(
            DockerImageName.parse("minio/minio:RELEASE.2025-04-22T22-12-26Z"))
            .withEnv("MINIO_ROOT_USER", ACCESS_KEY)
            .withEnv("MINIO_ROOT_PASSWORD", SECRET_KEY)
            .withCommand("server", "/data")
            .withExposedPorts(9000)
            .waitingFor(Wait.forHttp("/minio/health/live").forPort(9000));

    @TempDir
    Path temporaryDirectory;

    @Test
    void restoresAnArchiveAfterTheProducingWorkerDirectoryIsGone() throws Exception {
        MinioCheckpointStorage storage = new MinioCheckpointStorage(
                "http://" + MINIO.getHost() + ':' + MINIO.getMappedPort(9000),
                ACCESS_KEY, SECRET_KEY, "stream-checkpoints");
        Path producer = temporaryDirectory.resolve("worker-one/checkpoint-17");
        Path nested = producer.resolve("keyed-state/checkpoint-17/CURRENT");
        Files.createDirectories(nested.getParent());
        Files.writeString(nested, "manifest");
        Path envelope = producer.resolve("operator-task.bin");
        Files.writeString(envelope, "keyed-state/checkpoint-17");

        StateHandle durable = storage.publish("jobs/job/tasks/sessions/checkpoint-17.zip",
                producer, new StateHandle(envelope.toUri(), Files.size(envelope)));
        CheckpointArchives.deleteTree(producer);
        StateHandle restored = storage.materialize(durable,
                temporaryDirectory.resolve("worker-two/restore"));

        Path restoredEnvelope = Path.of(restored.uri());
        assertThat(restoredEnvelope).hasContent("keyed-state/checkpoint-17");
        assertThat(restoredEnvelope.getParent()
                .resolve("keyed-state/checkpoint-17/CURRENT")).hasContent("manifest");
    }
}
