package dev.dhruv.streaming.worker;

import dev.dhruv.streaming.runtime.OperatorTask;
import dev.dhruv.streaming.runtime.SourceTask;
import dev.dhruv.streaming.runtime.metrics.TaskMetricGroup;
import dev.dhruv.streaming.runtime.transport.Output;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.CompletableFuture;

/**
 * One task running on this worker, with what is needed to watch it and stop it.
 *
 * @param taskKey      the task's identity, {@code operatorId#subtaskIndex}
 * @param jobId        job this task belongs to, used to scope checkpoint injection
 * @param operatorId   the operator at the head of its chain
 * @param subtaskIndex which subtask
 * @param checkpointCompletionParticipant whether the chain contains this worker's sink tail
 * @param task         the runnable, a {@link SourceTask} or an {@link OperatorTask}
 * @param thread       the thread running it
 * @param output       its outgoing channels, closed when the task is cancelled
 * @param metrics      its metrics, reported on the heartbeat
 */
record RunningTask(
        String taskKey,
        String jobId,
        String operatorId,
        int subtaskIndex,
        boolean checkpointCompletionParticipant,
        Runnable task,
        Thread thread,
        Output output,
        ScheduledFuture<?> bufferFlush,
        TaskMetricGroup metrics
) {

    /**
     * Asks the task to stop.
     *
     * <p>Cooperative first, then an interrupt if it does not notice. A task blocked waiting for
     * credit that will never arrive -- because the worker it was sending to has died -- needs the
     * interrupt; one merely between records does not.
     */
    void cancel() {
        // The flusher can be blocked waiting for credit from a worker which just died. Interrupt
        // an in-flight run as well as cancelling future runs; RemoteSubpartition.close() also
        // wakes that credit wait so task teardown does not inherit its 120-second timeout.
        bufferFlush.cancel(true);
        switch (task) {
            case SourceTask source -> source.cancel();
            case OperatorTask operator -> operator.cancel();
            default -> throw new IllegalStateException("unknown task type " + task.getClass());
        }
        thread.interrupt();
    }

    /**
     * Returns whether the task's thread is still running.
     *
     * @return true while the thread is alive
     */
    boolean isAlive() {
        return thread.isAlive();
    }

    /** Distinguishes a completed task from the brief NEW state before its thread starts. */
    boolean isFinished() {
        return thread.getState() == Thread.State.TERMINATED;
    }

    /** Queues a completed-checkpoint callback on an operator task's own thread. */
    CompletableFuture<Void> notifyCheckpointComplete(long checkpointId) {
        if (task instanceof OperatorTask operator) {
            return operator.notifyCheckpointComplete(checkpointId);
        }
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Returns how many records this task has taken in.
     *
     * @return the records-in count, or records-out for a source
     */
    long recordsIn() {
        return metrics.snapshot().getOrDefault("records-in", 0L);
    }

    /**
     * Returns how many records this task has emitted.
     *
     * @return the records-out count
     */
    long recordsOut() {
        return metrics.snapshot().getOrDefault("records-out", 0L);
    }

    long lastCheckpointId() {
        return checkpointMetrics().checkpointId();
    }

    long lastCheckpointDurationMillis() {
        return checkpointMetrics().durationMillis();
    }

    long lastCheckpointStateBytes() {
        return checkpointMetrics().stateSizeBytes();
    }

    long lastAlignmentMillis() {
        return checkpointMetrics().alignmentMillis();
    }

    private CheckpointMetrics checkpointMetrics() {
        return switch (task) {
            case SourceTask source -> {
                SourceTask.CheckpointMetrics metrics = source.checkpointMetrics();
                yield new CheckpointMetrics(metrics.checkpointId(), metrics.durationMillis(),
                        metrics.stateSizeBytes(), metrics.alignmentMillis());
            }
            case OperatorTask operator -> {
                OperatorTask.CheckpointMetrics metrics = operator.checkpointMetrics();
                yield new CheckpointMetrics(metrics.checkpointId(), metrics.durationMillis(),
                        metrics.stateSizeBytes(), metrics.alignmentMillis());
            }
            default -> new CheckpointMetrics(0, 0, 0, 0);
        };
    }

    private record CheckpointMetrics(long checkpointId, long durationMillis,
                                     long stateSizeBytes, long alignmentMillis) {
    }
}
