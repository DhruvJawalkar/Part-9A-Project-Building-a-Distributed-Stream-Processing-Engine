package dev.dhruv.streaming.worker.checkpoint;

import dev.dhruv.streaming.api.state.StateHandle;
import io.minio.BucketExistsArgs;
import io.minio.GetObjectArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/** Durable task-checkpoint archive store backed by MinIO's S3-compatible API. */
public final class MinioCheckpointStorage implements CheckpointStorage {
    private static final long PART_SIZE_BYTES = 10L * 1024 * 1024;

    private final MinioClient client;
    private final String bucket;

    public MinioCheckpointStorage(String endpoint, String accessKey, String secretKey, String bucket) {
        this.client = MinioClient.builder()
                .endpoint(Objects.requireNonNull(endpoint, "endpoint"))
                .credentials(Objects.requireNonNull(accessKey, "accessKey"),
                        Objects.requireNonNull(secretKey, "secretKey"))
                .build();
        this.bucket = Objects.requireNonNull(bucket, "bucket");
    }

    @Override
    public StateHandle publish(String objectKey, Path checkpointDirectory, StateHandle entryPoint)
            throws IOException {
        Path archive = Files.createTempFile("stream-checkpoint-", ".zip");
        try {
            CheckpointArchives.archive(checkpointDirectory, archive);
            ensureBucket();
            long size = Files.size(archive);
            try (var input = Files.newInputStream(archive)) {
                client.putObject(PutObjectArgs.builder().bucket(bucket).object(objectKey)
                        .stream(input, size, PART_SIZE_BYTES)
                        .contentType("application/zip")
                        .build());
            } catch (Exception failure) {
                throw new IOException("could not upload checkpoint to MinIO: " + objectKey, failure);
            }
            String fragment = CheckpointArchives.entryPoint(checkpointDirectory, entryPoint);
            return new StateHandle(CheckpointArchives.withEntryPoint(
                    URI.create("minio://" + bucket + "/" + objectKey), fragment), size);
        } finally {
            Files.deleteIfExists(archive);
        }
    }

    @Override
    public StateHandle materialize(StateHandle durableHandle, Path targetDirectory) throws IOException {
        URI uri = durableHandle.uri();
        if (!"minio".equalsIgnoreCase(uri.getScheme()) || !bucket.equals(uri.getHost())) {
            throw new IOException("checkpoint does not belong to configured MinIO bucket '" + bucket + "': " + uri);
        }
        String object = uri.getPath();
        if (object == null || object.length() < 2) {
            throw new IOException("MinIO checkpoint URI has no object key: " + uri);
        }
        Path archive = Files.createTempFile("stream-checkpoint-", ".zip");
        try {
            try (var input = client.getObject(GetObjectArgs.builder().bucket(bucket)
                    .object(object.substring(1)).build())) {
                Files.copy(input, archive, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            } catch (Exception failure) {
                throw new IOException("could not download checkpoint from MinIO: " + uri, failure);
            }
            return CheckpointArchives.extract(archive, uri, targetDirectory, durableHandle.sizeBytes());
        } finally {
            Files.deleteIfExists(archive);
        }
    }

    private void ensureBucket() throws IOException {
        try {
            if (!client.bucketExists(BucketExistsArgs.builder().bucket(bucket).build())) {
                try {
                    client.makeBucket(MakeBucketArgs.builder().bucket(bucket).build());
                } catch (Exception racedCreator) {
                    // Several tasks can complete their first checkpoint together. A different
                    // worker winning creation is success, not a failed checkpoint.
                    if (!client.bucketExists(BucketExistsArgs.builder().bucket(bucket).build())) {
                        throw racedCreator;
                    }
                }
            }
        } catch (Exception failure) {
            throw new IOException("could not create or inspect MinIO bucket '" + bucket + "'", failure);
        }
    }
}
