package dev.dhruv.streaming.master.graph;

import dev.dhruv.streaming.api.ExchangeStrategy;
import dev.dhruv.streaming.api.KeyedOperator;
import dev.dhruv.streaming.api.graph.JobGraph;
import dev.dhruv.streaming.api.graph.LogicalOperator;
import dev.dhruv.streaming.api.graph.SinkNode;
import dev.dhruv.streaming.api.graph.SourceNode;
import dev.dhruv.streaming.api.graph.TransformNode;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Decides which adjacent operators can be fused into one thread.
 *
 * <h2>The four conditions</h2>
 *
 * <p>An operator {@code B} chains onto its upstream {@code A} when all of these hold. Each one
 * exists because dropping it would make the fusion either wrong or impossible, and it is worth
 * knowing which is which.
 *
 * <ol>
 *   <li><b>{@code B}'s input exchange is FORWARD.</b> Any other strategy means a record may
 *       have to reach a different subtask than the one that produced it, and a method call
 *       cannot cross subtasks. This is the condition that makes chaining <em>possible</em>.</li>
 *   <li><b>Equal parallelism.</b> Forward routing sends subtask {@code i} to subtask {@code i},
 *       which only exists on both sides when the counts match. (The graph builder has already
 *       downgraded unequal forward edges to rebalance, so this is belt and braces -- but the
 *       compiler should not depend on that having happened.)</li>
 *   <li><b>{@code A} has exactly one downstream.</b> An operator feeding two consumers cannot
 *       be fused into either, because its output has to reach both, and a fused call chain has
 *       one continuation.</li>
 *   <li><b>{@code B} has exactly one upstream.</b> A join has two inputs arriving on separate
 *       channels and, from Phase 4, has to align barriers across them. Fusing it into one of
 *       its inputs would hide the other.</li>
 * </ol>
 *
 * <p>Sources chain too, and that case is worth noticing rather than treating as incidental: a
 * source fused with the filter behind it means a record read from Kafka is filtered by the same
 * thread that read it, with no queue and no serialization in between. In the LMS job, that is
 * the difference between four threads and eight.
 */
public final class ChainBuilder {

    private ChainBuilder() {
    }

    /**
     * Groups the job's operators into chains.
     *
     * <p>Walks the graph in topological order, so that by the time an operator is considered,
     * the chain it might join has already been built. Every operator ends up in exactly one
     * group, including operators that chain with nothing and so form a group of one.
     *
     * @param graph the logical job graph
     * @return the chain groups, in topological order of their heads
     */
    public static List<ChainGroup> build(JobGraph graph) {
        List<List<LogicalOperator>> chains = new ArrayList<>();
        Set<String> alreadyChained = new LinkedHashSet<>();

        for (LogicalOperator operator : graph.operatorsInTopologicalOrder()) {
            if (alreadyChained.contains(operator.id())) {
                continue;
            }

            // Start a new chain at this operator and extend it forwards as far as the
            // conditions allow.
            List<LogicalOperator> chain = new ArrayList<>();
            chain.add(operator);
            alreadyChained.add(operator.id());

            Optional<LogicalOperator> next = chainableSuccessorOf(operator, graph);
            while (next.isPresent()) {
                LogicalOperator successor = next.get();
                chain.add(successor);
                alreadyChained.add(successor.id());
                next = chainableSuccessorOf(successor, graph);
            }

            chains.add(chain);
        }

        return chains.stream()
                .map(chain -> new ChainGroup(
                        chain.getFirst().id(), chain, chain.getFirst().parallelism()))
                .toList();
    }

    /**
     * Returns the operator that can be fused onto {@code operator}, if there is one.
     */
    private static Optional<LogicalOperator> chainableSuccessorOf(
            LogicalOperator operator, JobGraph graph) {

        List<String> downstreamIds = graph.downstreamIdsOf(operator.id());
        if (downstreamIds.size() != 1) {
            return Optional.empty();          // condition 3: no fan-out
        }

        LogicalOperator successor = graph.operator(downstreamIds.getFirst()).orElseThrow();
        if (successor.upstreamIds().size() != 1) {
            return Optional.empty();          // condition 4: no fan-in
        }
        if (successor.parallelism() != operator.parallelism()) {
            return Optional.empty();          // condition 2
        }
        if (inputExchangeOf(successor) != ExchangeStrategy.FORWARD) {
            return Optional.empty();          // condition 1
        }
        if (isKeyed(operator) || isKeyed(successor)) {
            // A keyed runtime owns its state backend, current-key context and timer service.
            // OperatorChain intentionally has one shared context, so fusing across this
            // boundary would silently give neighbouring operators the keyed operator's state.
            // Keep it a task boundary until contexts are namespaced per chained operator.
            return Optional.empty();
        }
        return Optional.of(successor);
    }

    /**
     * Returns how records reach an operator. A source has no input edge, and is reported as
     * forward so that it never blocks a chain it heads.
     *
     * @param operator the operator
     * @return its input exchange strategy
     */
    public static ExchangeStrategy inputExchangeOf(LogicalOperator operator) {
        return switch (operator) {
            case TransformNode transform -> transform.inputExchange();
            case SinkNode sink -> sink.inputExchange();
            case SourceNode ignored -> ExchangeStrategy.FORWARD;
        };
    }

    private static boolean isKeyed(LogicalOperator operator) {
        return switch (operator) {
            case TransformNode transform -> transform.keySelector().isPresent()
                    || transform.operator() instanceof KeyedOperator<?, ?, ?>;
            case SinkNode sink -> sink.keySelector().isPresent()
                    || sink.operator() instanceof KeyedOperator<?, ?, ?>;
            case SourceNode ignored -> false;
        };
    }
}
