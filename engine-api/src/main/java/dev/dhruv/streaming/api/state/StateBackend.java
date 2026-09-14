package dev.dhruv.streaming.api.state;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Where a task keeps its keyed state, and how that state is snapshotted and restored.
 *
 * <p>The interface is deliberately narrow. It has to be: the whole point is that swapping an
 * on-heap map implementation for an embedded RocksDB one changes the job's memory profile,
 * its GC behaviour and its maximum state size, while changing exactly zero lines of operator
 * code.
 *
 * <p>Only keyed state is modelled here. A task's own non-keyed state, such as a source's Kafka
 * offsets or a sink's list of pending files, is handled by those tasks directly. There are
 * precisely two such cases in this engine, and inventing an abstraction for two cases would
 * obscure more than it saves.
 */
public interface StateBackend extends AutoCloseable {

    /**
     * Sets the key that all subsequent state access is scoped to.
     *
     * <p>Called by the runtime, never by operator code. Every {@link ValueState} and
     * {@link ListState} handed out by this backend reads and writes against whatever key was
     * last passed here.
     *
     * @param key the key to make current
     */
    void setCurrentKey(Object key);

    /**
     * Returns the named value state, creating it on first request.
     *
     * <p>The same name always addresses the same underlying store, so calling this repeatedly
     * is cheap and safe. The type is passed for serialization, which starts to matter once the
     * backend stops being a map of live objects.
     *
     * @param stateName stable name, part of the checkpoint format. Renaming it orphans state.
     * @param type      the stored type
     * @param <T>       the stored type
     * @return value state scoped to the current key
     */
    <T> ValueState<T> valueState(String stateName, Class<T> type);

    /**
     * Returns the named list state, creating it on first request.
     *
     * @param stateName stable name, part of the checkpoint format
     * @param type      the element type
     * @param <T>       the element type
     * @return list state scoped to the current key
     */
    <T> ListState<T> listState(String stateName, Class<T> type);

    /**
     * Writes a snapshot of every key of state owned by this task and returns a handle to it.
     *
     * <p>Called on the task thread at the moment a checkpoint barrier has been aligned, so the
     * snapshot captures state as of exactly the pre-barrier prefix of every input channel. The
     * task is not processing records while this runs, which is why how long it takes is a
     * number worth reporting rather than hiding.
     *
     * @param checkpointId  the checkpoint being taken
     * @param checkpointDir directory to write into, unique to this checkpoint
     * @return a handle the master persists in the checkpoint metadata
     * @throws IOException if the snapshot cannot be written
     */
    StateHandle snapshot(long checkpointId, Path checkpointDir) throws IOException;

    /**
     * Replaces all local state with the contents of a previously written handle.
     *
     * <p>Called on a freshly deployed task before it processes its first record. Restoring is a
     * whole-state replacement, not a merge. The task is being rewound to a point in time, and
     * anything it accumulated since that point is by definition being discarded.
     *
     * @param handle a handle previously returned by {@link #snapshot}
     * @throws IOException if the snapshot cannot be read
     */
    void restore(StateHandle handle) throws IOException;

    /**
     * Releases any resources the backend holds. Narrowed from {@link AutoCloseable} so callers
     * are not forced to handle a checked exception on a cleanup path.
     */
    @Override
    void close();
}
