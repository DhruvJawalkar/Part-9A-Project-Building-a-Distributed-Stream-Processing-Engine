package dev.dhruv.streaming.runtime;

import dev.dhruv.streaming.api.ExchangeStrategy;
import dev.dhruv.streaming.api.StreamElement;
import dev.dhruv.streaming.api.StreamRecord;

import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * An {@link Output} that hands elements to other tasks in the same JVM through blocking queues.
 *
 * <p>This is Phase 1's entire network layer, and the queue is the interesting part of it. It is
 * bounded, so a task that cannot keep up stops its upstream from producing, which stops that
 * task's upstream, all the way back to the source -- which simply stops being polled. That is
 * backpressure, and it costs nothing to implement because it is what a bounded queue does when
 * you let it block instead of dropping or growing.
 *
 * <p>Phase 2 replaces this with a gRPC stream between worker processes, where the same
 * behaviour has to be rebuilt deliberately as credit-based flow control. A bounded queue gives
 * it away for free; a network does not.
 */
final class LocalOutput implements Output {

    private final List<BlockingQueue<StreamElement>> downstream;
    private final ExchangeStrategy strategy;
    private final int senderSubtaskIndex;

    private int nextRoundRobin;

    LocalOutput(List<BlockingQueue<StreamElement>> downstream,
                ExchangeStrategy strategy,
                int senderSubtaskIndex) {
        this.downstream = List.copyOf(downstream);
        this.strategy = strategy;
        this.senderSubtaskIndex = senderSubtaskIndex;
    }

    @Override
    public void emit(StreamRecord<?> record) throws InterruptedException {
        if (downstream.isEmpty()) {
            return;
        }
        switch (strategy) {
            // Subtask i to subtask i. The graph builder guarantees equal parallelism on a
            // forward edge, having downgraded any unequal one to REBALANCE.
            case FORWARD -> downstream.get(senderSubtaskIndex).put(record);

            case REBALANCE -> {
                downstream.get(nextRoundRobin).put(record);
                nextRoundRobin = (nextRoundRobin + 1) % downstream.size();
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
        if (strategy == ExchangeStrategy.FORWARD) {
            downstream.get(senderSubtaskIndex).put(element);
            return;
        }
        for (BlockingQueue<StreamElement> queue : downstream) {
            queue.put(element);
        }
    }

    @Override
    public void close() {
        // Nothing to release: the queues outlive this output and are owned by the executor.
    }
}
