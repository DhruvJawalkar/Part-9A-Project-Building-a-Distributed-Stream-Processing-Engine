package dev.dhruv.streaming.api;

import dev.dhruv.streaming.api.metrics.MetricGroup;

/**
 * What a source is told about the task running it.
 *
 * <p>A source operator runs at some parallelism, and each of its subtasks must read a disjoint
 * slice of the input. Nothing hands out those slices centrally, so every subtask works out its
 * own share from the two numbers here. A Kafka source assigns itself the partitions whose
 * index is congruent to {@link #subtaskIndex()} modulo {@link #parallelism()}; a file replay
 * source reads every line and keeps the ones that hash to its index.
 */
public interface SourceContext {

    /**
     * Returns this subtask's index within its operator, from zero.
     *
     * @return the subtask index
     */
    int subtaskIndex();

    /**
     * Returns how many subtasks this source is running as.
     *
     * @return the source operator's parallelism
     */
    int parallelism();

    /**
     * Returns the metric group for this subtask.
     *
     * @return this subtask's metrics
     */
    MetricGroup metrics();
}
