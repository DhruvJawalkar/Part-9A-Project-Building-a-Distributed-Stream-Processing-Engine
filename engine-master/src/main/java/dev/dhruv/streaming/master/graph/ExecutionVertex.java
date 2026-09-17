package dev.dhruv.streaming.master.graph;

import dev.dhruv.streaming.api.ExchangeStrategy;

import java.util.List;

/**
 * One parallel instance of one operator: the smallest thing the master schedules.
 *
 * <p>Where a {@link dev.dhruv.streaming.api.graph.LogicalOperator} says "there is a filter
 * running four ways", four of these say which four. The expansion from one to the other is the
 * first thing the compiler does, and it is the point at which parallelism stops being a number
 * and starts being a set of things that need somewhere to run.
 *
 * @param operatorId    the logical operator this instance belongs to
 * @param subtaskIndex  which instance, from zero
 * @param parallelism   how many instances there are in total
 * @param inputExchange how records reach this vertex from upstream
 * @param upstreamIds   the logical operators feeding it
 */
public record ExecutionVertex(
        String operatorId,
        int subtaskIndex,
        int parallelism,
        ExchangeStrategy inputExchange,
        List<String> upstreamIds
) {

    /**
     * Returns the stable identity of this vertex, as used in etcd and in log lines.
     *
     * @return {@code operatorId:subtaskIndex}
     */
    public String id() {
        return operatorId + ":" + subtaskIndex;
    }
}
