package dev.dhruv.streaming.api;

import java.io.Serializable;

/**
 * Optional lifecycle for an operator that participates in a checkpoint beyond keyed state.
 *
 * <p>The runtime calls {@link #preCommit(long)} on the task thread after its input barrier has
 * aligned and before it writes the task checkpoint. The returned value is stored in that task
 * checkpoint and passed back to {@link #restoreCheckpointState(Serializable)} after
 * {@link Operator#open(OperatorContext)} during recovery. This makes resources opened in
 * {@code open} available while a transactional sink rebuilds its pending work.
 *
 * <p>After every task acknowledgement has made a checkpoint durable, the coordinator calls
 * {@link #notifyCheckpointComplete(long)}. A checkpoint that cannot complete instead reaches
 * {@link #notifyCheckpointAborted(long)}. Completion messages may be replayed after recovery,
 * so implementations must make completion idempotent.
 */
public interface CheckpointListener extends Serializable {

    /**
     * Finishes the current checkpoint interval and returns the state needed to recover it.
     *
     * @param checkpointId the barrier being snapshotted
     * @return serializable operator-owned checkpoint state; {@code null} is allowed when none
     */
    default Serializable preCommit(long checkpointId) throws Exception {
        return null;
    }

    /**
     * Restores state returned by {@link #preCommit(long)} after the operator has opened.
     *
     * @param checkpointState state from the recovered task checkpoint, possibly {@code null}
     */
    default void restoreCheckpointState(Serializable checkpointState) throws Exception {
    }

    /**
     * Announces that a checkpoint is durable and its externally visible work may commit.
     *
     * @param checkpointId the completed checkpoint
     */
    default void notifyCheckpointComplete(long checkpointId) throws Exception {
    }

    /**
     * Announces that a checkpoint will never complete.
     *
     * @param checkpointId the abandoned checkpoint
     */
    default void notifyCheckpointAborted(long checkpointId) throws Exception {
    }
}
