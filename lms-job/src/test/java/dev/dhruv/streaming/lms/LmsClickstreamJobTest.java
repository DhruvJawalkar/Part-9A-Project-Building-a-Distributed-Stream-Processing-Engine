package dev.dhruv.streaming.lms;

import dev.dhruv.streaming.api.ExchangeStrategy;
import dev.dhruv.streaming.api.graph.JobGraph;
import dev.dhruv.streaming.api.graph.LogicalOperator;
import dev.dhruv.streaming.api.graph.SinkNode;
import dev.dhruv.streaming.api.graph.SourceNode;
import dev.dhruv.streaming.api.graph.TransformNode;
import dev.dhruv.streaming.runtime.SerializationUtil;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that the job describes the pipeline it means to.
 *
 * <p>Nothing here starts an engine or touches Kafka. Building a graph is pure description, and
 * being able to assert on it without running anything is a property worth having: it is what
 * makes the graph a thing the master can validate, serialize and schedule before a single task
 * exists.
 */
class LmsClickstreamJobTest {

    @Test
    @DisplayName("describes the session and click-to-borrow conversion branches")
    void graphShape() {
        JobGraph graph = LmsClickstreamJob.buildGraph();

        assertThat(graph.name()).isEqualTo("lms-clickstream");
        assertThat(graph.operators()).extracting(LogicalOperator::id)
                .containsExactly("clicks", "borrows", "drop-bots", "sessions", "console",
                        "result-clicks", "tag-result-clicks", "tag-borrows",
                        "conversion-inputs", "conversions", "conversion-console");
        assertThat(graph.sources()).extracting(LogicalOperator::id)
                .containsExactly("clicks", "borrows");
        assertThat(graph.sinks()).extracting(LogicalOperator::id)
                .containsExactly("console", "conversion-console");
    }

    @Test
    @DisplayName("configures event time on the source")
    void eventTimeIsConfigured() {
        var sources = LmsClickstreamJob.buildGraph().sources();
        SourceNode clicks = sources.getFirst();
        SourceNode borrows = sources.get(1);

        assertThat(clicks.timestampAssigner())
                .as("without an assigner, no window downstream could ever fire")
                .isPresent();
        assertThat(clicks.outOfOrderness()).isEqualTo(Duration.ofSeconds(5));
        assertThat(clicks.idleTimeout())
                .as("idleness must be configured, or one quiet partition stalls the whole job")
                .isEqualTo(Duration.ofSeconds(30));
        assertThat(borrows.timestampAssigner()).isPresent();
        assertThat(borrows.outOfOrderness()).isEqualTo(Duration.ofSeconds(5));
        assertThat(borrows.idleTimeout()).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    @DisplayName("keeps the filter on a forward edge so it can be chained")
    void filterIsChainable() {
        JobGraph graph = LmsClickstreamJob.buildGraph();

        LogicalOperator filter = graph.operator("drop-bots").orElseThrow();
        assertThat(filter.parallelism())
                .as("equal parallelism plus a forward edge is what makes chaining possible")
                .isEqualTo(graph.operator("clicks").orElseThrow().parallelism());
    }

    @Test
    @DisplayName("narrows into the sink by rebalance rather than forward")
    void sinkNarrowsByRebalance() {
        // Four session subtasks into two sink subtasks. A forward edge would leave session
        // subtasks 2 and 3 with nowhere to send, so the builder downgrades the edge.
        SinkNode console = LmsClickstreamJob.buildGraph().sinks().getFirst();

        assertThat(console.parallelism()).isEqualTo(2);
        assertThat(console.inputExchange()).isEqualTo(ExchangeStrategy.REBALANCE);
    }

    @Test
    @DisplayName("hashes member events into the session aggregator")
    void sessionsAreKeyedByMember() {
        LogicalOperator sessions = LmsClickstreamJob.buildGraph().operator("sessions").orElseThrow();

        assertThat(sessions).isInstanceOf(TransformNode.class);
        TransformNode sessionNode = (TransformNode) sessions;
        assertThat(sessionNode.inputExchange()).isEqualTo(ExchangeStrategy.HASH);
        assertThat(sessionNode.partitionName()).contains("by-member");
    }

    @Test
    @DisplayName("unions tagged inputs before hashing conversions by member and item")
    void conversionsAreAKeyedTwoSidedIntervalJoin() {
        JobGraph graph = LmsClickstreamJob.buildGraph();
        TransformNode union = (TransformNode) graph.operator("conversion-inputs").orElseThrow();
        TransformNode conversions = (TransformNode) graph.operator("conversions").orElseThrow();

        assertThat(union.upstreamIds())
                .containsExactly("tag-result-clicks", "tag-borrows");
        assertThat(union.inputExchange()).isEqualTo(ExchangeStrategy.REBALANCE);
        assertThat(conversions.upstreamIds()).containsExactly("conversion-inputs");
        assertThat(conversions.inputExchange()).isEqualTo(ExchangeStrategy.HASH);
        assertThat(conversions.partitionName()).contains("by-member-and-item");
        assertThat(conversions.operator())
                .isInstanceOf(dev.dhruv.streaming.api.IntervalJoinOperator.class);
    }

    @Test
    @DisplayName("serializes the complete two-branch job for remote submission")
    void graphWithJoinIsSerializable() {
        JobGraph graph = LmsClickstreamJob.buildGraph();

        JobGraph restored = SerializationUtil.fromBytes(SerializationUtil.toBytes(graph));

        assertThat(restored.operators()).extracting(LogicalOperator::id)
                .containsExactlyElementsOf(graph.operators().stream()
                        .map(LogicalOperator::id).toList());
        assertThat(restored.operator("conversions")).isPresent();
    }
}
