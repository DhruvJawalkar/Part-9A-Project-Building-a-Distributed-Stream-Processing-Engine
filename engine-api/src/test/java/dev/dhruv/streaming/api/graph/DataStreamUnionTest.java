package dev.dhruv.streaming.api.graph;

import dev.dhruv.streaming.api.Collector;
import dev.dhruv.streaming.api.ExchangeStrategy;
import dev.dhruv.streaming.api.KeyedOperator;
import dev.dhruv.streaming.api.Operator;
import dev.dhruv.streaming.api.Source;
import dev.dhruv.streaming.api.SourceContext;
import dev.dhruv.streaming.api.StreamRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the typed, two-input stream union primitive. */
class DataStreamUnionTest {

    @Test
    @DisplayName("creates one rebalance transform with both upstreams")
    void createsFanInNode() {
        JobGraph.Builder builder = JobGraph.named("union");
        DataStream<String> left = builder.source("left", emptySource()).parallelism(2);
        DataStream<String> right = builder.source("right", emptySource()).parallelism(3);

        DataStream<String> merged = left.union("merged", right);
        merged.keyBy("by-value", value -> value)
                .process("join", new PassThroughKeyedOperator())
                .sink("out", (record, out) -> {
                });

        JobGraph graph = builder.build();
        TransformNode union = (TransformNode) graph.operator("merged").orElseThrow();

        assertThat(graph.operators()).extracting(LogicalOperator::id)
                .containsExactly("left", "right", "merged", "join", "out");
        assertThat(union.upstreamIds()).containsExactly("left", "right");
        assertThat(union.inputExchange()).isEqualTo(ExchangeStrategy.REBALANCE);
        assertThat(union.parallelism()).isEqualTo(3);
        assertThat(graph.downstreamIdsOf("left")).containsExactly("merged");
        assertThat(graph.downstreamIdsOf("right")).containsExactly("merged");
        assertThat(graph.operatorsInTopologicalOrder()).extracting(LogicalOperator::id)
                .containsSubsequence("left", "merged", "join", "out")
                .containsSubsequence("right", "merged");
    }

    @Test
    @DisplayName("forwards the value and event timestamp unchanged")
    @SuppressWarnings("unchecked")
    void preservesRecordsAndTimestamps() throws Exception {
        JobGraph.Builder builder = JobGraph.named("union-identity");
        DataStream<String> left = builder.source("left", emptySource());
        DataStream<String> merged = left.union("merged", builder.source("right", emptySource()));
        TransformNode union = (TransformNode) builder.build().operator(merged.id()).orElseThrow();

        List<StreamRecord<String>> output = new ArrayList<>();
        Collector<String> collector = new Collector<>() {
            @Override
            public void collect(String value) {
                output.add(new StreamRecord<>(value, Long.MIN_VALUE));
            }

            @Override
            public void collect(String value, long timestamp) {
                output.add(new StreamRecord<>(value, timestamp));
            }
        };
        ((Operator<String, String>) union.operator())
                .processElement(new StreamRecord<>("event", 1234L), collector);

        assertThat(output).containsExactly(new StreamRecord<>("event", 1234L));
    }

    @Test
    @DisplayName("rejects streams belonging to different job builders")
    void rejectsCrossJobUnion() {
        DataStream<String> left = JobGraph.named("first").source("left", emptySource());
        DataStream<String> right = JobGraph.named("second").source("right", emptySource());

        assertThatThrownBy(() -> left.union("merged", right))
                .isInstanceOf(InvalidJobGraphException.class)
                .hasMessageContaining("different JobGraph builders")
                .hasMessageContaining("same job");
    }

    @Test
    @DisplayName("rejects unioning a stream with itself")
    void rejectsSelfUnion() {
        DataStream<String> stream = JobGraph.named("self-union")
                .source("input", emptySource());

        assertThatThrownBy(() -> stream.union("merged", stream))
                .isInstanceOf(InvalidJobGraphException.class)
                .hasMessageContaining("with itself")
                .hasMessageContaining("two distinct upstream streams");
    }

    @Test
    @DisplayName("retains both event-time branches for downstream watermark minima")
    void retainsBothEventTimeBranches() {
        JobGraph.Builder builder = JobGraph.named("union-watermarks");
        DataStream<String> left = builder.source("left", emptySource())
                .withEventTime(String::length, Duration.ofSeconds(2));
        DataStream<String> right = builder.source("right", emptySource())
                .withEventTime(String::length, Duration.ofSeconds(7));
        left.union("merged", right).sink("out", (record, out) -> {
        });

        JobGraph graph = builder.build();
        TransformNode union = (TransformNode) graph.operator("merged").orElseThrow();

        // The two source channels remain explicit inputs to the rebalance node. Runtime
        // watermark tracking therefore computes the minimum over both branches, rather than
        // allowing an identity operator to hide either source.
        assertThat(union.upstreamIds()).containsExactly("left", "right");
        assertThat(graph.sources()).extracting(SourceNode::id).containsExactly("left", "right");
        assertThat(graph.sources()).extracting(SourceNode::outOfOrderness)
                .containsExactly(Duration.ofSeconds(2), Duration.ofSeconds(7));
    }

    @Test
    @DisplayName("round-trips through Java serialization")
    void serializes() throws Exception {
        JobGraph.Builder builder = JobGraph.named("union-serialization");
        builder.source("left", emptySource())
                .union("merged", builder.source("right", emptySource()))
                .sink("out", (record, out) -> {
                });
        JobGraph original = builder.build();

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(original);
        }
        JobGraph restored;
        try (ObjectInputStream input = new ObjectInputStream(
                new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (JobGraph) input.readObject();
        }

        assertThat(restored.name()).isEqualTo(original.name());
        assertThat(restored.operators()).extracting(LogicalOperator::id)
                .containsExactlyElementsOf(original.operators().stream()
                        .map(LogicalOperator::id).toList());
        TransformNode union = (TransformNode) restored.operator("merged").orElseThrow();
        assertThat(union.inputExchange()).isEqualTo(ExchangeStrategy.REBALANCE);
        assertThat(union.upstreamIds()).containsExactly("left", "right");
    }

    private static Source<String> emptySource() {
        return new Source<>() {
            @Override
            public void open(SourceContext context) {
            }

            @Override
            public boolean poll(Collector<String> out) {
                return false;
            }

            @Override
            public void close() {
            }
        };
    }

    private static final class PassThroughKeyedOperator
            implements KeyedOperator<String, String, String> {

        @Override
        public void processElement(StreamRecord<String> record, Collector<String> out) {
            out.collect(record.value());
        }

        @Override
        public void onEventTimer(long timestamp, String key, Collector<String> out) {
        }
    }
}
