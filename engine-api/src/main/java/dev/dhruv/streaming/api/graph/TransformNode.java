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
 * A node knows how records reach it; an upstream node emitting to two different downstreams may
 * well be doing two different things with the same records. Attaching the strategy to the
 * consumer is what keeps that expressible.
 *
 * <p>The nullable components with {@code Optional} accessors are there for the reason given in
 * {@link SourceNode}: this graph is serialized, and {@link Optional} is not.
 *
 * @param id                  stable operator id
 * @param parallelism         number of parallel subtasks
 * @param upstreamIds         the operators feeding this one, in the order the operator expects
 * @param inputExchange       how records are routed from upstream subtasks to this node's
 * @param operator            the user logic, serialized to whichever worker runs it
 * @param keySelectorOrNull   how to extract the partitioning key; non-null exactly when
 *                            {@code inputExchange} is {@link ExchangeStrategy#HASH}, and required
 *                            then, since without it there is nothing to hash. Prefer
 *                            {@link #keySelector()}.
 * @param partitionNameOrNull the user-supplied name of the keying step, carried purely so that
 *                            logs and metrics can say "by-member" rather than "hash exchange".
 *                            Prefer {@link #partitionName()}.
 */
public record TransformNode(
        String id,
        int parallelism,
        List<String> upstreamIds,
        ExchangeStrategy inputExchange,
        Operator<?, ?> operator,
        KeySelector<?, ?> keySelectorOrNull,
        String partitionNameOrNull
) implements LogicalOperator {

    private static final long serialVersionUID = 1L;

    /**
     * Returns how to extract this operator's partitioning key, if it is keyed.
     *
     * @return the key selector, if any
     */
    public Optional<KeySelector<?, ?>> keySelector() {
        return Optional.ofNullable(keySelectorOrNull);
    }

    /**
     * Returns the user's name for the keying step, if there was one.
     *
     * @return the partition name, if any
     */
    public Optional<String> partitionName() {
        return Optional.ofNullable(partitionNameOrNull);
    }
}
