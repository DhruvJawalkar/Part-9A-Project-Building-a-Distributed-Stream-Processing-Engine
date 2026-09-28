package dev.dhruv.streaming.metadata;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The one recovery point a restarted job is allowed to use.
 *
 * <p>A checkpoint is deliberately a job-wide object, even though every task writes its own
 * state. A collection with one missing task is not a consistent cut through the dataflow, so
 * this value is only written after the coordinator has received every expected acknowledgement.
 * Keeping that rule next to the durable representation makes it difficult for a future caller
 * to accidentally turn a partial checkpoint into a recovery point.
 *
 * @param checkpointId checkpoint sequence number within its job
 * @param triggerTimestamp wall-clock time at which the coordinator injected source barriers
 * @param taskStates state and observability data keyed by {@code operatorId:subtaskIndex}
 */
public record CompletedCheckpoint(
        long checkpointId,
        long triggerTimestamp,
        Map<String, TaskState> taskStates
) {

    public CompletedCheckpoint {
        if (checkpointId < 1) {
            throw new IllegalArgumentException("checkpointId must be positive");
        }
        Objects.requireNonNull(taskStates, "taskStates");
        taskStates = Map.copyOf(new LinkedHashMap<>(taskStates));
        if (taskStates.isEmpty()) {
            throw new IllegalArgumentException("a completed checkpoint needs at least one task");
        }
    }

    /**
     * One task's contribution to a consistent checkpoint.
     *
     * @param stateHandleUri opaque URI understood by the state backend
     * @param stateSizeBytes bytes written by this task
     * @param alignmentMillis time this task spent waiting for its input barriers
     */
    public record TaskState(String stateHandleUri, long stateSizeBytes, long alignmentMillis) {
        public TaskState {
            Objects.requireNonNull(stateHandleUri, "stateHandleUri");
            if (stateSizeBytes < 0 || alignmentMillis < 0) {
                throw new IllegalArgumentException("checkpoint measurements must not be negative");
            }
        }
    }
}
