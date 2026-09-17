package dev.dhruv.streaming.worker;

import dev.dhruv.streaming.runtime.OperatorTask;
import dev.dhruv.streaming.runtime.SourceTask;
import dev.dhruv.streaming.runtime.metrics.TaskMetricGroup;
import dev.dhruv.streaming.runtime.transport.ResultPartitionWriter;

/**
 * One task running on this worker, with what is needed to watch it and stop it.
 *
 * @param taskKey      the task's identity, {@code operatorId#subtaskIndex}
 * @param operatorId   the operator at the head of its chain
 * @param subtaskIndex which subtask
 * @param task         the runnable, a {@link SourceTask} or an {@link OperatorTask}
 * @param thread       the thread running it
 * @param output       its outgoing channels, closed when the task is cancelled
 * @param metrics      its metrics, reported on the heartbeat
 */
record RunningTask(
        String taskKey,
        String operatorId,
        int subtaskIndex,
        Runnable task,
        Thread thread,
        ResultPartitionWriter output,
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
}
