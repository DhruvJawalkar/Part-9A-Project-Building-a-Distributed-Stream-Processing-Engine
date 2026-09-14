package dev.dhruv.streaming.lms;

import dev.dhruv.streaming.api.ExchangeStrategy;
import dev.dhruv.streaming.api.graph.JobGraph;
import dev.dhruv.streaming.api.graph.LogicalOperator;
import dev.dhruv.streaming.api.graph.SinkNode;
import dev.dhruv.streaming.api.graph.SourceNode;
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
    @DisplayName("describes source, filter and sink")
    void graphShape() {
        JobGraph graph = LmsClickstreamJob.buildGraph();

        assertThat(graph.name()).isEqualTo("lms-clickstream");
        assertThat(graph.operators()).extracting(LogicalOperator::id)
                .containsExactly("clicks", "drop-bots", "console");
        assertThat(graph.sources()).extracting(LogicalOperator::id).containsExactly("clicks");
        assertThat(graph.sinks()).extracting(LogicalOperator::id).containsExactly("console");
    }

    @Test
    @DisplayName("configures event time on the source")
    void eventTimeIsConfigured() {
        SourceNode clicks = LmsClickstreamJob.buildGraph().sources().getFirst();

        assertThat(clicks.timestampAssigner())
                .as("without an assigner, no window downstream could ever fire")
                .isPresent();
        assertThat(clicks.outOfOrderness()).isEqualTo(Duration.ofSeconds(5));
        assertThat(clicks.idleTimeout())
                .as("idleness must be configured, or one quiet partition stalls the whole job")
                .isEqualTo(Duration.ofSeconds(30));
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
        // Four filters into two sinks. A forward edge would leave filter subtasks 2 and 3 with
        // nowhere to send, so the builder downgrades the edge.
        SinkNode console = LmsClickstreamJob.buildGraph().sinks().getFirst();

        assertThat(console.parallelism()).isEqualTo(2);
        assertThat(console.inputExchange()).isEqualTo(ExchangeStrategy.REBALANCE);
    }

    @Test
    @DisplayName("uses no keyed exchange yet")
    void nothingIsKeyedYet() {
        // Phase 3 adds the session aggregator and with it the first hash exchange. Until then
        // the job is embarrassingly parallel, which is why Phase 1 can run it without a key
        // group in sight.
        assertThat(LmsClickstreamJob.buildGraph().operators())
                .noneMatch(op -> op instanceof SinkNode sink
                        && sink.inputExchange() == ExchangeStrategy.HASH);
    }
}
