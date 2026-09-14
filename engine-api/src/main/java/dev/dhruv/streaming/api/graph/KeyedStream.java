package dev.dhruv.streaming.api.graph;

import dev.dhruv.streaming.api.ExchangeStrategy;
import dev.dhruv.streaming.api.KeySelector;
import dev.dhruv.streaming.api.KeyedOperator;
import dev.dhruv.streaming.api.Operator;

import java.util.Objects;

/**
 * A stream that has been partitioned by key, and so can carry operators that hold keyed state.
 *
 * <p>Distinct from {@link DataStream} at the type level on purpose. Keyed state only makes
 * sense where the engine guarantees that every record for a key reaches the same subtask, and
 * that guarantee is exactly what keying establishes. Making {@link KeyedOperator} placeable
 * only here means the guarantee is enforced by the compiler rather than by remembering to key
 * first.
 *
 * <p>Nothing has happened yet when a keyed stream is created. It holds the selector and waits:
 * the {@link ExchangeStrategy#HASH} exchange it implies is applied to the next operator's
 * input edge.
 *
 * @param <K> the key type
 * @param <T> the record type
 */
public final class KeyedStream<K, T> {

    private final JobGraph.Builder builder;
    private final GraphNode upstream;
    private final KeySelector<T, K> selector;
    private final String partitionName;

    KeyedStream(JobGraph.Builder builder, GraphNode upstream,
                KeySelector<T, K> selector, String partitionName) {
        this.builder = builder;
        this.upstream = upstream;
        this.selector = selector;
        this.partitionName = partitionName;
    }

    /**
     * Adds a keyed operator to this stream.
     *
     * <p>The operator may use keyed state and register event-time timers, because the hash
     * exchange placed on its input edge guarantees it sees every record for any key it sees at
     * all.
     *
     * @param id       stable operator id, unique within the job
     * @param operator the user logic
     * @param <R>      the type it produces
     * @return a handle to the records it produces
     */
    public <R> DataStream<R> process(String id, KeyedOperator<K, T, R> operator) {
        GraphNode next = builder.addNode(id, GraphNode.Kind.TRANSFORM);
        next.operator = Objects.requireNonNull(operator, "operator");
        next.parallelism = upstream.parallelism;
        next.upstreamIds.add(upstream.id);
        next.inputExchange = ExchangeStrategy.HASH;
        next.keySelector = selector;
        next.partitionName = partitionName;
        return new DataStream<>(builder, next);
    }

    /**
     * Terminates this stream in a sink that receives records grouped by key.
     *
     * @param id   stable operator id, unique within the job
     * @param sink the sink logic, an operator that produces nothing
     * @return a handle on which the sink's parallelism may be set
     */
    public DataStreamSink sink(String id, Operator<T, Void> sink) {
        GraphNode next = builder.addNode(id, GraphNode.Kind.SINK);
        next.operator = Objects.requireNonNull(sink, "sink");
        next.parallelism = upstream.parallelism;
        next.upstreamIds.add(upstream.id);
        next.inputExchange = ExchangeStrategy.HASH;
        next.keySelector = selector;
        next.partitionName = partitionName;
        return new DataStreamSink(next);
    }
}
