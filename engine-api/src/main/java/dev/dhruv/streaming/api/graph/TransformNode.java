package dev.dhruv.streaming.api.graph;

import dev.dhruv.streaming.api.ExchangeStrategy;
import dev.dhruv.streaming.api.KeySelector;
import dev.dhruv.streaming.api.Operator;

import java.util.List;
import java.util.Optional;

/**
 * A node that consumes records and produces records.
 *
 * <p>Note that the exchange strategy is a property of a node's <em>input</em>, not its output.
 * A node knows how records reach it; an upstream node emitting to two different downstreams
 * may well be doing two different things with the same records. Attaching the strategy to the
 * consumer is what keeps that expressible.
 *
 * @param id            stable operator id
 * @param parallelism   number of parallel subtasks
 * @param upstreamIds   the operators feeding this one, in the order the operator expects them
 * @param inputExchange how records are routed from upstream subtasks to this node's subtasks
 * @param operator      the user logic, serialized to whichever worker runs it
 * @param keySelector   how to extract the partitioning key; present exactly when
 *                      {@code inputExchange} is {@link ExchangeStrategy#HASH}, and required
 *                      then, since without it there is nothing to hash
 * @param partitionName the user-supplied name of the keying step, carried purely so that logs
 *                      and metrics can say "by-member" rather than "hash exchange"
 */
public record TransformNode(
        String id,
        int parallelism,
        List<String> upstreamIds,
        ExchangeStrategy inputExchange,
        Operator<?, ?> operator,
        Optional<KeySelector<?, ?>> keySelector,
        Optional<String> partitionName
) implements LogicalOperator {
}
