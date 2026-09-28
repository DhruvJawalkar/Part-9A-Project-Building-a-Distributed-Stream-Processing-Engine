package dev.dhruv.streaming.worker.checkpoint;

import dev.dhruv.streaming.api.state.StateHandle;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Durable storage for one complete task checkpoint.
 *
 * <p>The runtime writes a checkpoint locally first because RocksDB and sources need ordinary
 * files while taking a consistent cut. The worker then archives that <em>entire</em> directory
 * as one immutable object. A checkpoint handle consequently remains valid when recovery assigns
 * the task to a different worker with a different local disk.
 */
public interface CheckpointStorage {

    /** Publishes a task checkpoint directory and returns a durable archive handle. */
    StateHandle publish(String objectKey, Path checkpointDirectory, StateHandle entryPoint)
            throws IOException;

    /** Materializes an archive on this worker and returns a local handle to its entry point. */
    StateHandle materialize(StateHandle durableHandle, Path targetDirectory) throws IOException;
}
