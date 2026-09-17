package dev.dhruv.streaming.api.graph;

import dev.dhruv.streaming.api.Collector;
import dev.dhruv.streaming.api.ExchangeStrategy;
import dev.dhruv.streaming.api.Operator;
import dev.dhruv.streaming.api.Source;
import dev.dhruv.streaming.api.SourceContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for job graph construction and validation.
 *
 * <p>Validation lives on the record's constructor rather than the builder, so these tests come
 * in two flavours: those that go through the fluent API, and those that assemble node records
 * directly. The second kind exists because some invalid graphs are unreachable through the
 * builder -- a cycle, in particular, cannot be described by an API where every step appends
 * downstream of an existing handle. The check still has to be there: the graph will also be
 * read back from etcd in a later phase, and that route has no builder to go through.
 */
class JobGraphTest {

    @Nested
    @DisplayName("rejects graphs that could not run")
    class Rejects {

        @Test
        @DisplayName("a cycle")
        void cyclicGraph() {
            // source -> a -> b -> a. Assembled directly; the builder cannot express this.
            List<LogicalOperator> operators = List.of(
                    source("clicks"),
                    transform("a", List.of("clicks", "b")),
                    transform("b", List.of("a")));

            assertThatThrownBy(() -> new JobGraph("job-1", "cyclic", 128, operators))
                    .isInstanceOf(InvalidJobGraphException.class)
                    .hasMessageContaining("cycle");
        }

        @Test
        @DisplayName("an operator at parallelism zero, through the builder")
        void zeroParallelismViaBuilder() {
            JobGraph.Builder builder = JobGraph.named("zero-parallelism");
            builder.source("clicks", noopSource())
                    .filter("drop-bots", value -> true).parallelism(0)
                    .sink("out", printSink());

            assertThatThrownBy(builder::build)
                    .isInstanceOf(InvalidJobGraphException.class)
                    .hasMessageContaining("drop-bots")
                    .hasMessageContaining("parallelism 0");
        }

        @Test
        @DisplayName("an operator at negative parallelism")
        void negativeParallelism() {
            List<LogicalOperator> operators = List.of(
                    source("clicks"),
                    new TransformNode("a", -3, List.of("clicks"), ExchangeStrategy.FORWARD,
                            passThrough(), null, null));

            assertThatThrownBy(() -> new JobGraph("job-1", "negative", 128, operators))
                    .isInstanceOf(InvalidJobGraphException.class)
                    .hasMessageContaining("at least 1");
        }

        @Test
        @DisplayName("parallelism above the job maximum")
        void parallelismAboveMax() {
            List<LogicalOperator> operators = List.of(
                    source("clicks"),
                    new TransformNode("a", 9, List.of("clicks"), ExchangeStrategy.FORWARD,
                            passThrough(), null, null));

            assertThatThrownBy(() -> new JobGraph("job-1", "too-parallel", 8, operators))
                    .isInstanceOf(InvalidJobGraphException.class)
                    .hasMessageContaining("maxParallelism");
        }

        @Test
        @DisplayName("two operators sharing an id")
        void duplicateIds() {
            JobGraph.Builder builder = JobGraph.named("duplicate-ids");
            DataStream<String> clicks = builder.source("clicks", noopSource());
            clicks.filter("same", value -> true);

            assertThatThrownBy(() -> clicks.filter("same", value -> true))
                    .isInstanceOf(InvalidJobGraphException.class)
                    .hasMessageContaining("already has an operator named 'same'");
        }

        @Test
        @DisplayName("an operator unreachable from any source")
        void unreachableOperator() {
            // 'island' reads from 'orphan', and nothing feeds 'orphan' from a source.
            List<LogicalOperator> operators = List.of(
                    source("clicks"),
                    transform("connected", List.of("clicks")),
                    transform("orphan", List.of("island")),
                    transform("island", List.of("orphan")));

            assertThatThrownBy(() -> new JobGraph("job-1", "island", 128, operators))
                    .isInstanceOf(InvalidJobGraphException.class);
        }

        @Test
        @DisplayName("a graph with no source at all")
        void noSource() {
            List<LogicalOperator> operators = List.of(transform("a", List.of("a")));

            assertThatThrownBy(() -> new JobGraph("job-1", "sourceless", 128, operators))
                    .isInstanceOf(InvalidJobGraphException.class);
        }

        @Test
        @DisplayName("a hash exchange with no key selector")
        void hashWithoutKeySelector() {
            List<LogicalOperator> operators = List.of(
                    source("clicks"),
                    new TransformNode("sessions", 1, List.of("clicks"), ExchangeStrategy.HASH,
                            passThrough(), null, null));

            assertThatThrownBy(() -> new JobGraph("job-1", "unhashable", 128, operators))
                    .isInstanceOf(InvalidJobGraphException.class)
                    .hasMessageContaining("no key selector");
        }

        @Test
        @DisplayName("event time declared on something that is not a source")
        void eventTimeOffSource() {
            JobGraph.Builder builder = JobGraph.named("late-event-time");
            DataStream<String> filtered =
                    builder.source("clicks", noopSource()).filter("f", value -> true);

            assertThatThrownBy(() -> filtered.withEventTime(String::length, Duration.ZERO))
                    .isInstanceOf(InvalidJobGraphException.class)
                    .hasMessageContaining("only valid on a source");
        }
    }

    @Nested
    @DisplayName("accepts and describes a well-formed graph")
    class Accepts {

        @Test
        @DisplayName("a linear source-filter-sink job")
        void linearJob() {
            JobGraph.Builder builder = JobGraph.named("lms-clickstream");
            builder.source("clicks", noopSource())
                    .withEventTime(String::length, Duration.ofSeconds(5))
                    .parallelism(4)
                    .filter("drop-bots", value -> !value.startsWith("bot"))
                    .parallelism(4)
                    .sink("console", printSink())
                    .parallelism(2);

            JobGraph graph = builder.build();

            assertThat(graph.name()).isEqualTo("lms-clickstream");
            assertThat(graph.operators()).hasSize(3);
            assertThat(graph.sources()).extracting(LogicalOperator::id).containsExactly("clicks");
            assertThat(graph.sinks()).extracting(LogicalOperator::id).containsExactly("console");
            assertThat(graph.operator("drop-bots")).get()
                    .extracting(LogicalOperator::parallelism).isEqualTo(4);
            assertThat(graph.operator("console")).get()
                    .extracting(LogicalOperator::parallelism).isEqualTo(2);
            assertThat(graph.downstreamIdsOf("clicks")).containsExactly("drop-bots");
        }

        @Test
        @DisplayName("carrying event-time configuration on the source")
        void eventTimeConfiguration() {
            JobGraph.Builder builder = JobGraph.named("event-time");
            builder.source("clicks", noopSource())
                    .withEventTime(String::length, Duration.ofSeconds(5))
                    .withIdleness(Duration.ofSeconds(30))
                    .sink("out", printSink());

            SourceNode clicks = builder.build().sources().getFirst();

            assertThat(clicks.timestampAssigner()).isPresent();
            assertThat(clicks.outOfOrderness()).isEqualTo(Duration.ofSeconds(5));
            assertThat(clicks.idleTimeout()).isEqualTo(Duration.ofSeconds(30));
        }

        @Test
        @DisplayName("marking a keyed edge as a hash exchange")
        void keyByProducesHashExchange() {
            JobGraph.Builder builder = JobGraph.named("keyed");
            builder.source("clicks", noopSource())
                    .keyBy("by-member", value -> value)
                    .process("sessions", new CountingKeyedOperator())
                    .sink("out", printSink());

            TransformNode sessions = (TransformNode) builder.build().operator("sessions")
                    .orElseThrow();

            assertThat(sessions.inputExchange()).isEqualTo(ExchangeStrategy.HASH);
            assertThat(sessions.keySelector()).isPresent();
            assertThat(sessions.partitionName()).contains("by-member");
        }

        @Test
        @DisplayName("keyBy adds no operator of its own")
        void keyByIsNotAnOperator() {
            JobGraph.Builder builder = JobGraph.named("keyed");
            builder.source("clicks", noopSource())
                    .keyBy("by-member", value -> value)
                    .process("sessions", new CountingKeyedOperator())
                    .sink("out", printSink());

            // source, sessions, out -- and nothing named "by-member". Keying is a property of
            // an edge, so materialising it as a task would mean a thread that only rehashes.
            assertThat(builder.build().operators()).extracting(LogicalOperator::id)
                    .containsExactly("clicks", "sessions", "out");
        }

        @Test
        @DisplayName("ordering operators after their upstreams")
        void topologicalOrder() {
            JobGraph.Builder builder = JobGraph.named("ordered");
            DataStream<String> clicks = builder.source("clicks", noopSource());
            clicks.filter("a", value -> true).filter("b", value -> true).sink("out", printSink());

            List<String> order = builder.build().operatorsInTopologicalOrder().stream()
                    .map(LogicalOperator::id).toList();

            assertThat(order).containsExactly("clicks", "a", "b", "out");
        }

        @Test
        @DisplayName("a stream consumed twice, which is why handles exist")
        void fanOut() {
            JobGraph.Builder builder = JobGraph.named("fan-out");
            DataStream<String> clicks = builder.source("clicks", noopSource());
            clicks.filter("bots", value -> true).sink("bot-out", printSink());
            clicks.filter("humans", value -> true).sink("human-out", printSink());

            JobGraph graph = builder.build();

            assertThat(graph.downstreamIdsOf("clicks")).containsExactly("bots", "humans");
            assertThat(graph.sinks()).hasSize(2);
        }
    }

    // -------------------------------------------------------------------------------------
    // Fixtures
    // -------------------------------------------------------------------------------------

    private static SourceNode source(String id) {
        return new SourceNode(id, 1, noopSource(), null,
                Duration.ZERO, Duration.ZERO);
    }

    private static TransformNode transform(String id, List<String> upstreamIds) {
        return new TransformNode(id, 1, upstreamIds, ExchangeStrategy.FORWARD,
                passThrough(), null, null);
    }

    private static Operator<String, String> passThrough() {
        return (record, out) -> out.collect(record.value());
    }

    private static Operator<String, Void> printSink() {
        return (record, out) -> {
        };
    }

    private static Source<String> noopSource() {
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

    /** A keyed operator, needed because {@code KeyedOperator} has two abstract methods. */
    private static final class CountingKeyedOperator
            implements dev.dhruv.streaming.api.KeyedOperator<String, String, String> {

        @Override
        public void processElement(dev.dhruv.streaming.api.StreamRecord<String> record,
                                   Collector<String> out) {
            out.collect(record.value());
        }

        @Override
        public void onEventTimer(long timestamp, String key, Collector<String> out) {
        }
    }
}
