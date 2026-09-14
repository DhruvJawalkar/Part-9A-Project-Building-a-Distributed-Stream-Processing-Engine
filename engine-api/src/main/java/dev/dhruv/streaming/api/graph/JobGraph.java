package dev.dhruv.streaming.api.graph;

import dev.dhruv.streaming.api.ExchangeStrategy;
import dev.dhruv.streaming.api.KeyGroupAssigner;
import dev.dhruv.streaming.api.Source;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * The job as the user described it: operators, how they connect, and how parallel each is.
 *
 * <p>This is the <em>logical</em> graph. It says there is an operator called {@code "sessions"}
 * running four ways; it says nothing about which worker any of those four lands on. The master
 * compiles this into a physical execution graph, and the two are kept as separate types rather
 * than one type that gets mutated, so that the distinction stays legible in the source.
 *
 * <p>Instances are immutable and validated on construction. Every rule is checked here rather
 * than in the builder, so a graph assembled by any route -- the fluent API, a test, a future
 * deserialization from etcd -- is subject to the same checks.
 *
 * @param jobId          unique id for this submission
 * @param name           human-readable job name
 * @param maxParallelism ceiling on any operator's parallelism
 * @param operators      every node in the graph
 */
public record JobGraph(
        String jobId,
        String name,
        int maxParallelism,
        List<LogicalOperator> operators
) {

    /**
     * Validates and freezes the graph.
     *
     * @throws InvalidJobGraphException if the graph could not be run
     */
    public JobGraph {
        Objects.requireNonNull(jobId, "jobId");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(operators, "operators");
        operators = List.copyOf(operators);
        validate(name, maxParallelism, operators);
    }

    /**
     * Starts describing a job.
     *
     * @param name human-readable job name
     * @return a builder
     */
    public static Builder named(String name) {
        return new Builder(name);
    }

    /**
     * Looks up an operator by id.
     *
     * @param id the operator id
     * @return the operator, if the graph has one by that name
     */
    public Optional<LogicalOperator> operator(String id) {
        return operators.stream().filter(op -> op.id().equals(id)).findFirst();
    }

    /**
     * Returns the source nodes. These are where checkpoint barriers are injected and where
     * event time enters the job.
     *
     * @return every source, in declaration order
     */
    public List<SourceNode> sources() {
        return operators.stream().filter(SourceNode.class::isInstance)
                .map(SourceNode.class::cast).toList();
    }

    /**
     * Returns the sink nodes. These are the tasks told that a checkpoint completed.
     *
     * @return every sink, in declaration order
     */
    public List<SinkNode> sinks() {
        return operators.stream().filter(SinkNode.class::isInstance)
                .map(SinkNode.class::cast).toList();
    }

    /**
     * Returns the ids of operators fed by the given one.
     *
     * <p>Edges are stored on the consumer as {@code upstreamIds}, so answering the downstream
     * question means scanning. The graph has tens of nodes, not thousands, and a scan that
     * needs no index to stay correct is the better trade here.
     *
     * @param id the upstream operator id
     * @return ids of its downstream operators, in declaration order
     */
    public List<String> downstreamIdsOf(String id) {
        return operators.stream().filter(op -> op.upstreamIds().contains(id))
                .map(LogicalOperator::id).toList();
    }

    /**
     * Returns the operators ordered so that every operator appears after all of its upstreams.
     *
     * <p>The order the execution graph compiler walks the job in. Guaranteed to exist and to
     * include every node, because construction already rejected cycles.
     *
     * @return every operator in topological order
     */
    public List<LogicalOperator> operatorsInTopologicalOrder() {
        return topologicalOrder(operators).orElseThrow(
                () -> new IllegalStateException("validated graph is cyclic: " + name));
    }

    // ---------------------------------------------------------------------------------
    // Validation
    // ---------------------------------------------------------------------------------

    private static void validate(String name, int maxParallelism, List<LogicalOperator> ops) {
        if (ops.isEmpty()) {
            throw new InvalidJobGraphException("job '" + name + "' has no operators");
        }
        if (maxParallelism < 1 || maxParallelism > KeyGroupAssigner.NUM_KEY_GROUPS) {
            throw new InvalidJobGraphException(
                    "job '" + name + "' has maxParallelism " + maxParallelism
                            + "; must be between 1 and " + KeyGroupAssigner.NUM_KEY_GROUPS
                            + " (the number of key groups)");
        }

        Map<String, LogicalOperator> byId = new LinkedHashMap<>();
        for (LogicalOperator op : ops) {
            if (byId.put(op.id(), op) != null) {
                throw new InvalidJobGraphException(
                        "job '" + name + "' has two operators named '" + op.id()
                                + "'; operator ids must be unique, since checkpointed state is"
                                + " filed under them");
            }
        }

        for (LogicalOperator op : ops) {
            if (op.parallelism() < 1) {
                throw new InvalidJobGraphException(
                        "operator '" + op.id() + "' has parallelism " + op.parallelism()
                                + "; must be at least 1");
            }
            if (op.parallelism() > maxParallelism) {
                throw new InvalidJobGraphException(
                        "operator '" + op.id() + "' has parallelism " + op.parallelism()
                                + ", above the job maxParallelism of " + maxParallelism);
            }
            for (String upstream : op.upstreamIds()) {
                if (!byId.containsKey(upstream)) {
                    throw new InvalidJobGraphException(
                            "operator '" + op.id() + "' reads from '" + upstream
                                    + "', which is not an operator in this job");
                }
            }
            if (!(op instanceof SourceNode) && op.upstreamIds().isEmpty()) {
                throw new InvalidJobGraphException(
                        "operator '" + op.id() + "' has no input and is not a source");
            }
            requireKeySelectorWhenHashed(op);
        }

        if (ops.stream().noneMatch(SourceNode.class::isInstance)) {
            throw new InvalidJobGraphException(
                    "job '" + name + "' has no source; records have no way to enter it");
        }

        // Cycles. A stream job is a DAG: records flow one way, and a cycle would mean a task
        // waiting on output it is itself responsible for producing.
        if (topologicalOrder(ops).isEmpty()) {
            throw new InvalidJobGraphException(
                    "job '" + name + "' contains a cycle; a job graph must be acyclic");
        }

        // Reachability. Catches a disconnected island of operators, which is always a wiring
        // mistake rather than an intent -- those tasks would be scheduled and would then sit
        // idle forever, waiting for input nothing sends them.
        Set<String> reachable = reachableFromSources(ops);
        for (LogicalOperator op : ops) {
            if (!reachable.contains(op.id())) {
                throw new InvalidJobGraphException(
                        "operator '" + op.id() + "' is not reachable from any source");
            }
        }
    }

    private static void requireKeySelectorWhenHashed(LogicalOperator op) {
        ExchangeStrategy exchange;
        boolean hasSelector;
        switch (op) {
            case TransformNode t -> {
                exchange = t.inputExchange();
                hasSelector = t.keySelector().isPresent();
            }
            case SinkNode s -> {
                exchange = s.inputExchange();
                hasSelector = s.keySelector().isPresent();
            }
            case SourceNode ignored -> {
                return;
            }
        }
        if (exchange == ExchangeStrategy.HASH && !hasSelector) {
            throw new InvalidJobGraphException(
                    "operator '" + op.id() + "' uses a HASH exchange but has no key selector");
        }
        if (exchange != ExchangeStrategy.HASH && hasSelector) {
            throw new InvalidJobGraphException(
                    "operator '" + op.id() + "' has a key selector but a " + exchange
                            + " exchange; a key selector only means something when hashing");
        }
    }

    /**
     * Kahn's algorithm. Returns empty when the graph has a cycle, signalled by the ready queue
     * draining before every node has been emitted.
     */
    private static Optional<List<LogicalOperator>> topologicalOrder(List<LogicalOperator> ops) {
        Map<String, Integer> remainingInputs = new HashMap<>();
        for (LogicalOperator op : ops) {
            remainingInputs.put(op.id(), (int) op.upstreamIds().stream().distinct().count());
        }

        Deque<LogicalOperator> ready = new ArrayDeque<>();
        for (LogicalOperator op : ops) {
            if (remainingInputs.get(op.id()) == 0) {
                ready.add(op);
            }
        }

        List<LogicalOperator> ordered = new ArrayList<>(ops.size());
        while (!ready.isEmpty()) {
            LogicalOperator op = ready.poll();
            ordered.add(op);
            for (LogicalOperator candidate : ops) {
                if (candidate.upstreamIds().contains(op.id())) {
                    int left = remainingInputs.merge(candidate.id(), -1, Integer::sum);
                    if (left == 0) {
                        ready.add(candidate);
                    }
                }
            }
        }

        return ordered.size() == ops.size() ? Optional.of(ordered) : Optional.empty();
    }

    private static Set<String> reachableFromSources(List<LogicalOperator> ops) {
        Deque<String> frontier = new ArrayDeque<>();
        Set<String> seen = new HashSet<>();
        for (LogicalOperator op : ops) {
            if (op instanceof SourceNode) {
                frontier.add(op.id());
                seen.add(op.id());
            }
        }
        while (!frontier.isEmpty()) {
            String current = frontier.poll();
            for (LogicalOperator op : ops) {
                if (op.upstreamIds().contains(current) && seen.add(op.id())) {
                    frontier.add(op.id());
                }
            }
        }
        return seen;
    }

    // ---------------------------------------------------------------------------------
    // Builder
    // ---------------------------------------------------------------------------------

    /**
     * Assembles a job graph.
     *
     * <p>Each step returns a typed handle to the stream it produced, which the next step is
     * called on. Holding a handle in a variable is how a stream gets consumed twice, and how
     * the two sides of a join are named -- neither of which a single unbranching chain of calls
     * can express.
     */
    public static final class Builder {

        private final String name;
        private final Map<String, GraphNode> nodes = new LinkedHashMap<>();
        private int maxParallelism = KeyGroupAssigner.NUM_KEY_GROUPS;

        private Builder(String name) {
            this.name = Objects.requireNonNull(name, "name");
        }

        /**
         * Sets the ceiling on any operator's parallelism.
         *
         * <p>Fixed at job creation and effectively permanent: it bounds how a key maps to a
         * subtask, so changing it invalidates every existing checkpoint. Defaults to
         * {@link KeyGroupAssigner#NUM_KEY_GROUPS}.
         *
         * <p>Note that in this engine the key group count itself is a compile-time constant, so
         * lowering this only lowers the cap; it does not change how keys are grouped.
         *
         * @param maxParallelism the ceiling, at most the number of key groups
         * @return this builder
         */
        public Builder maxParallelism(int maxParallelism) {
            this.maxParallelism = maxParallelism;
            return this;
        }

        /**
         * Adds a source.
         *
         * @param id     stable operator id, unique within the job
         * @param source the source implementation
         * @param <T>    the record type it produces
         * @return a handle to the stream of records it produces
         */
        public <T> DataStream<T> source(String id, Source<T> source) {
            GraphNode node = addNode(id, GraphNode.Kind.SOURCE);
            node.source = Objects.requireNonNull(source, "source");
            return new DataStream<>(this, node);
        }

        /**
         * Validates the description and produces the graph.
         *
         * @return the immutable, validated job graph
         * @throws InvalidJobGraphException if the graph could not be run
         */
        public JobGraph build() {
            downgradeForwardEdgesWithUnequalParallelism();
            List<LogicalOperator> frozen =
                    nodes.values().stream().map(GraphNode::freeze).toList();
            return new JobGraph(UUID.randomUUID().toString(), name, maxParallelism, frozen);
        }

        /**
         * Turns a forward edge between operators of different parallelism into a rebalance.
         *
         * <p>A forward exchange means subtask {@code i} sends to subtask {@code i}, which is
         * only meaningful when there is a subtask {@code i} on both ends. Four filters feeding
         * two sinks -- which is what {@code .sink(...).parallelism(2)} after a parallelism-four
         * operator describes -- has no such correspondence, and subtasks two and three would
         * have nowhere to send.
         *
         * <p>Rather than reject that, which would make the natural way of writing a narrower
         * sink an error, the edge is downgraded to round-robin. It is safe here because a
         * forward edge is never keyed: keying sets {@link ExchangeStrategy#HASH} instead, and a
         * hash edge is left alone precisely because rebalancing one would break the grouping
         * guarantee that keyed state depends on.
         */
        private void downgradeForwardEdgesWithUnequalParallelism() {
            for (GraphNode node : nodes.values()) {
                if (node.inputExchange != ExchangeStrategy.FORWARD) {
                    continue;
                }
                for (String upstreamId : node.upstreamIds) {
                    GraphNode upstream = nodes.get(upstreamId);
                    if (upstream != null && upstream.parallelism != node.parallelism) {
                        node.inputExchange = ExchangeStrategy.REBALANCE;
                        break;
                    }
                }
            }
        }

        GraphNode addNode(String id, GraphNode.Kind kind) {
            Objects.requireNonNull(id, "id");
            GraphNode node = new GraphNode(id, kind);
            if (nodes.putIfAbsent(id, node) != null) {
                throw new InvalidJobGraphException(
                        "job '" + name + "' already has an operator named '" + id + "'");
            }
            return node;
        }
    }
}
