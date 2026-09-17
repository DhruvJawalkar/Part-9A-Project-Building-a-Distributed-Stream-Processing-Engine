package dev.dhruv.streaming.runtime;

import dev.dhruv.streaming.api.SourceContext;
import dev.dhruv.streaming.api.metrics.MetricGroup;

/**
 * What a source subtask is told about itself.
 *
 * @param subtaskIndex this subtask's index within its operator
 * @param parallelism  how many subtasks the source operator runs as
 * @param metrics      this subtask's metric group
 */
public record RuntimeSourceContext(int subtaskIndex, int parallelism, MetricGroup metrics)
        implements SourceContext {
}
