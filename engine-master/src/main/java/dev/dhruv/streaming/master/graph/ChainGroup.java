package dev.dhruv.streaming.master.graph;

import dev.dhruv.streaming.api.graph.LogicalOperator;

import java.util.List;

/**
 * A run of adjacent operators fused into a single task, running on one thread.
 *
 * <p>Chaining is where most of a stream engine's throughput comes from, and it is worth being
 * precise about what it removes. Two operators on either side of an ordinary edge exchange
 * records by serializing each one, putting it in a buffer, handing the buffer to another
 * thread, and deserializing on the far side. Two operators in a chain exchange records by
 * calling a method. Same graph, same semantics, and roughly two orders of magnitude between
 * them on a cheap operator.
 *
 * <p>The cost is that a chain is scheduled as a unit: it cannot be split across workers, and
 * one slow operator in a chain blocks the others, because they share the thread. That is the
 * right trade almost always, and it is why the conditions for chaining are narrow.
 *
 * @param id          identity of the group, taken from its head operator so that it is stable
 *                    across recompiles and readable in logs
 * @param operators   the fused operators, in the order records flow through them
 * @param parallelism how many parallel copies of this chain run; equal for every operator in
 *                    it, by construction
 */
public record ChainGroup(String id, List<LogicalOperator> operators, int parallelism) {

    /**
     * Compact constructor, defensively copying and rejecting an empty group.
     */
    public ChainGroup {
        operators = List.copyOf(operators);
        if (operators.isEmpty()) {
            throw new IllegalArgumentException("a chain group must contain at least one operator");
        }
    }

    /**
     * Returns the first operator in the chain, which is what records enter through.
     *
     * @return the head operator
     */
    public LogicalOperator head() {
        return operators.getFirst();
    }

    /**
     * Returns the last operator in the chain, which is what records leave through.
     *
     * @return the tail operator
     */
    public LogicalOperator tail() {
        return operators.getLast();
    }

    /**
     * Returns whether this group is more than one operator, meaning fusion actually happened.
     *
     * @return true if two or more operators are chained here
     */
    public boolean isChained() {
        return operators.size() > 1;
    }

    /**
     * Returns the operator ids in this group, for logging and assertions.
     *
     * @return the ids, in flow order
     */
    public List<String> operatorIds() {
        return operators.stream().map(LogicalOperator::id).toList();
    }
}
