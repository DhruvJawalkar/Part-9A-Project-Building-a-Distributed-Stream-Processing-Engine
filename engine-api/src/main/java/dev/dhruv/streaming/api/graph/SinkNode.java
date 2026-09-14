package dev.dhruv.streaming.api.graph;

import dev.dhruv.streaming.api.ExchangeStrategy;
import dev.dhruv.streaming.api.KeySelector;
import dev.dhruv.streaming.api.Operator;

import java.util.List;
import java.util.Optional;

/**
 * A node that consumes records and produces nothing downstream.
 *
 * <p>A sink is an {@link Operator} whose output type is {@link Void}, not a separate kind of
 * thing. That keeps the run loop uniform: a sink task runs the same loop as any other task and
 * simply has nowhere to collect to.
 *
 * <p>It is a distinct node kind all the same, because two mechanisms need to find sinks
 * specifically. The checkpoint coordinator notifies them, and only them, when a checkpoint
 * completes -- that notification is the commit signal of the engine's two-phase commit. A sink
 * identified merely as "a node with no downstream" would work, but the graph would stop
 * recording that the author meant this to be an output.
 *
 * @param id            stable operator id
 * @param parallelism   number of parallel subtasks
 * @param upstreamIds   the operators feeding this sink
 * @param inputExchange how records are routed to this sink's subtasks
 * @param operator      the sink logic, serialized to whichever worker runs it
 * @param keySelector   present exactly when {@code inputExchange} is
 *                      {@link ExchangeStrategy#HASH}
 * @param partitionName the user-supplied name of the keying step, for logs and metrics
 */
public record SinkNode(
        String id,
        int parallelism,
        List<String> upstreamIds,
        ExchangeStrategy inputExchange,
        Operator<?, Void> operator,
        Optional<KeySelector<?, ?>> keySelector,
        Optional<String> partitionName
) implements LogicalOperator {
}
