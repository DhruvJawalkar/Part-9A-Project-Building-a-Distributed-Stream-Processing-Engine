package dev.dhruv.streaming.api;

import dev.dhruv.streaming.api.state.StateHandle;

import java.io.IOException;
import java.nio.file.Path;

/**
 * A source whose read position is part of an engine checkpoint.
 *
 * <p>A source owns the one piece of task state that is not keyed operator state: its input
 * position.  The checkpoint runtime snapshots that position immediately before it emits a
 * barrier.  On recovery it restores the position before the first poll, so the source and its
 * downstream operator state resume from the same logical point in the stream.
 *
 * <p>This interface deliberately does not expose broker commits.  A Kafka consumer group
 * offset is an external, independently timed side effect and therefore cannot describe a
 * consistent checkpoint.  Implementations must instead write and restore their own position
 * from the supplied {@link StateHandle}.
 *
 * @param <T> the record type produced by the source
 */
public interface CheckpointableSource<T> extends Source<T> {

    /**
     * Writes this source's next read position for a checkpoint.
     *
     * @param checkpointId  monotonically increasing checkpoint id
     * @param checkpointDir task-specific directory in durable checkpoint storage
     * @return a handle to the written source state
     * @throws IOException if the position cannot be persisted
     */
    StateHandle snapshot(long checkpointId, Path checkpointDir) throws IOException;

    /**
     * Restores the next read position from a completed checkpoint.
     *
     * <p>The runtime invokes this after {@link #open(SourceContext)} has acquired source
     * resources, but before it invokes {@link #poll(Collector)} for the first time.
     *
     * @param handle a handle returned by {@link #snapshot(long, Path)}
     * @throws IOException if the saved position cannot be read or applied
     */
    void restore(StateHandle handle) throws IOException;
}
