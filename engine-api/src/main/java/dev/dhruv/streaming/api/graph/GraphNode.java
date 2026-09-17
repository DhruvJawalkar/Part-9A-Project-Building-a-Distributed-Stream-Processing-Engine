package dev.dhruv.streaming.api.graph;

import dev.dhruv.streaming.api.ExchangeStrategy;
import dev.dhruv.streaming.api.KeySelector;
import dev.dhruv.streaming.api.Operator;
import dev.dhruv.streaming.api.Source;
import dev.dhruv.streaming.api.TimestampAssigner;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * A node while the graph is still being written: mutable, permissive, and not yet checked.
 *
 * <p>This exists because a fluent builder inherently describes a node across several calls --
 * {@code .filter(...)} creates it and {@code .parallelism(4)} amends it -- while the graph the
 * engine runs on must be immutable and valid. Rather than blur the two, the builder collects
 * these and {@link #freeze()} turns each into the corresponding {@link LogicalOperator} record
 * once the user is done describing it.
 *
 * <p>Package-private on purpose. Nothing outside the builder should ever hold a graph node it
 * can still change.
 */
final class GraphNode {

    /** Which of the three {@link LogicalOperator} kinds this node will freeze into. */
    enum Kind { SOURCE, TRANSFORM, SINK }

    final String id;
    final Kind kind;
    final List<String> upstreamIds = new ArrayList<>();

    int parallelism = 1;
    ExchangeStrategy inputExchange = ExchangeStrategy.FORWARD;

    Operator<?, ?> operator;
    Source<?> source;

    KeySelector<?, ?> keySelector;
    String partitionName;

    TimestampAssigner<?> timestampAssigner;
    Duration outOfOrderness = Duration.ZERO;
    Duration idleTimeout = Duration.ZERO;

    GraphNode(String id, Kind kind) {
        this.id = id;
        this.kind = kind;
    }

    /**
     * Converts this node into its immutable form.
     *
     * @return the frozen node
     */
    @SuppressWarnings("unchecked")
    LogicalOperator freeze() {
        return switch (kind) {
            case SOURCE -> new SourceNode(
                    id,
                    parallelism,
                    source,
                    timestampAssigner,
                    outOfOrderness,
                    idleTimeout);
            case TRANSFORM -> new TransformNode(
                    id,
                    parallelism,
                    List.copyOf(upstreamIds),
                    inputExchange,
                    operator,
                    keySelector,
                    partitionName);
            case SINK -> new SinkNode(
                    id,
                    parallelism,
                    List.copyOf(upstreamIds),
                    inputExchange,
                    (Operator<?, Void>) operator,
                    keySelector,
                    partitionName);
        };
    }
}
