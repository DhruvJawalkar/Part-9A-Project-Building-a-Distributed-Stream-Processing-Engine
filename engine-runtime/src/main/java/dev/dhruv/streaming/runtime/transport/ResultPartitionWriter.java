package dev.dhruv.streaming.runtime.transport;

import dev.dhruv.streaming.api.ExchangeStrategy;
import dev.dhruv.streaming.api.StreamElement;
import dev.dhruv.streaming.api.StreamRecord;

import java.util.List;

/**
 * Everything one task sends, split into one subpartition per downstream subtask.
 *
 * <p>This is where the exchange strategy stops being an enum and starts being a routing
 * decision. A record arrives, the strategy says which downstream subtask should get it, and the
 * corresponding subpartition ships it -- across a queue if that subtask is on this worker, over
 * the network with credit if it is not.
 *
 * <p>The writer does not know or care which of those two it is doing. That is the point of
 * {@link ResultSubpartition}: placement is the scheduler's decision, and nothing in the routing
 * logic should have to be rewritten because a task moved.
 */
public final class ResultPartitionWriter implements Output {

    private final List<ResultSubpartition> subpartitions;
    private final ExchangeStrategy strategy;
    private final int senderSubtaskIndex;

    private int nextRoundRobin;

    /**
     * Creates a writer.
     *
     * @param subpartitions      one per downstream subtask, indexed by that subtask's index
     * @param strategy           how records are routed across them
     * @param senderSubtaskIndex this task's own index, which forward routing sends to
     */
    public ResultPartitionWriter(List<ResultSubpartition> subpartitions,
                                 ExchangeStrategy strategy,
                                 int senderSubtaskIndex) {
        this.subpartitions = List.copyOf(subpartitions);
        this.strategy = strategy;
        this.senderSubtaskIndex = senderSubtaskIndex;
    }

    @Override
    public void emit(StreamRecord<?> record) throws InterruptedException {
        if (subpartitions.isEmpty()) {
            return;                            // a sink: nothing downstream to route to
        }
        switch (strategy) {
            // Subtask i to subtask i. The graph builder guarantees equal parallelism on a
            // forward edge, having downgraded any unequal one to REBALANCE.
            case FORWARD -> subpartitions.get(senderSubtaskIndex).add(record);

            case REBALANCE -> {
                subpartitions.get(nextRoundRobin).add(record);
                nextRoundRobin = (nextRoundRobin + 1) % subpartitions.size();
            }

            case BROADCAST -> broadcast(record);

            // Phase 3 fills this in, routing through KeyGroupAssigner.subtaskFor. It needs the
            // edge's key selector, which a keyed exchange is required to carry.
            case HASH -> throw new UnsupportedOperationException(
                    "hash exchange arrives in Phase 3, together with keyed state");
        }
    }

    @Override
    public void broadcast(StreamElement element) throws InterruptedException {
        // "Every downstream subtask" means every one this sender actually feeds, which under a
        // forward exchange is exactly one. Sending a watermark down channels that carry no
        // records from here would be a claim about a stream this task does not produce; a
        // barrier sent the same way would be worse, arriving on a channel that will never
        // deliver the records it is supposed to be separating.
        if (strategy == ExchangeStrategy.FORWARD && !subpartitions.isEmpty()) {
            subpartitions.get(senderSubtaskIndex).add(element);
            return;
        }
        for (ResultSubpartition subpartition : subpartitions) {
            subpartition.add(element);
        }
    }

    @Override
    public void flush() throws InterruptedException {
        for (ResultSubpartition subpartition : subpartitions) {
            subpartition.flush();
        }
    }

    @Override
    public void close() {
        subpartitions.forEach(ResultSubpartition::close);
    }
}
