package dev.dhruv.streaming.api;

/**
 * How records are routed from the subtasks of one operator to the subtasks of the next.
 *
 * <p>This is the single most consequential property of an edge in the job graph. It decides
 * whether the edge can be chained away entirely, whether it costs a serialization and a
 * network hop, and -- for anything keyed -- whether the job is correct at all.
 */
public enum ExchangeStrategy {

    /**
     * One-to-one with the co-located downstream subtask: subtask {@code i} sends only to
     * subtask {@code i}.
     *
     * <p>The only chainable strategy. When two adjacent operators have equal parallelism and a
     * forward exchange between them, the edge is not a network edge at all. The runtime fuses
     * them into one task and the "exchange" becomes a method call, with no serialization, no
     * buffer and no queue. Most of the throughput of a real engine comes from this.
     */
    FORWARD,

    /**
     * Partition by key hash, so that every record sharing a key reaches the same downstream
     * subtask.
     *
     * <p>The prerequisite for keyed state. It guarantees grouping, which people reliably
     * expect, and it guarantees nothing whatever about load balance, which people reliably do
     * not. A key carrying sixty percent of the traffic lands sixty percent of the traffic on
     * one subtask, and the job stays up while one thread drowns.
     *
     * <p>Routing goes through {@link KeyGroupAssigner} rather than straight to
     * {@code hash % parallelism}, so that the mapping survives a change of parallelism.
     */
    HASH,

    /**
     * Every record to every downstream subtask.
     */
    BROADCAST,

    /**
     * Round-robin across downstream subtasks, for even load.
     *
     * <p>Never valid before a keyed operator. It balances load precisely by destroying the
     * grouping guarantee, which means the same key lands on different subtasks over time and
     * keyed state silently splits. When a hash exchange produces skew, rebalancing is the
     * intuitive fix and the wrong one; changing the key distribution is the right one.
     */
    REBALANCE
}
