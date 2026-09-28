package dev.dhruv.streaming.worker.checkpoint;

import dev.dhruv.streaming.api.state.StateHandle;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Archive store for unit tests and single-machine development.
 *
 * <p>It has exactly the same archive/materialization semantics as MinIO, making it useful for
 * tests without pretending a local checkpoint directory itself is durable cross-worker state.
 */
public final class FileSystemCheckpointStorage implements CheckpointStorage {
    private final Path root;

    public FileSystemCheckpointStorage(Path root) {
        this.root = Objects.requireNonNull(root, "root").toAbsolutePath().normalize();
    }

    @Override
    public StateHandle publish(String objectKey, Path checkpointDirectory, StateHandle entryPoint)
            throws IOException {
        Path archive = archivePath(objectKey);
        Files.createDirectories(root);
        Path staging = Files.createTempFile(root, "checkpoint-", ".zip");
        try {
            CheckpointArchives.archive(checkpointDirectory, staging);
            Files.createDirectories(archive.getParent());
            Files.move(staging, archive, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            String fragment = CheckpointArchives.entryPoint(checkpointDirectory, entryPoint);
            return new StateHandle(CheckpointArchives.withEntryPoint(archive.toUri(), fragment),
                    Files.size(archive));
        } finally {
            Files.deleteIfExists(staging);
        }
    }

    @Override
    public StateHandle materialize(StateHandle durableHandle, Path targetDirectory) throws IOException {
        URI uri = durableHandle.uri();
        if (!"file".equalsIgnoreCase(uri.getScheme())) {
            throw new IOException("filesystem checkpoint store cannot read " + uri);
        }
        return CheckpointArchives.extract(Path.of(withoutFragment(uri)), uri, targetDirectory,
                durableHandle.sizeBytes());
    }

    private Path archivePath(String objectKey) throws IOException {
        Path relative = Path.of(Objects.requireNonNull(objectKey, "objectKey")).normalize();
        if (relative.isAbsolute() || relative.startsWith("..")) {
            throw new IOException("checkpoint object key escapes storage root: " + objectKey);
        }
        Path result = root.resolve(relative).normalize();
        if (!result.startsWith(root)) {
            throw new IOException("checkpoint object key escapes storage root: " + objectKey);
        }
        return result;
    }

    private static URI withoutFragment(URI uri) {
        return URI.create(uri.toString().substring(0, uri.toString().indexOf('#') >= 0
                ? uri.toString().indexOf('#') : uri.toString().length()));
    }
}
