package dev.dhruv.streaming.master.graph;

import dev.dhruv.streaming.api.Collector;
import dev.dhruv.streaming.api.KeyedOperator;
import dev.dhruv.streaming.api.Operator;
import dev.dhruv.streaming.api.Source;
import dev.dhruv.streaming.api.SourceContext;
import dev.dhruv.streaming.api.StreamRecord;
import dev.dhruv.streaming.api.graph.DataStream;
import dev.dhruv.streaming.api.graph.JobGraph;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for operator chaining.
 *
 * <p>Each test here isolates one of the four conditions by breaking exactly that one, so that
 * a failure says which rule stopped applying rather than just that chaining changed.
 */
class ChainBuilderTest {

    @Test
    @DisplayName("fuses a forward edge at equal parallelism")
    void chainsForwardEqualParallelism() {
        // The acceptance criterion: filter and map at equal parallelism, one thread, no
        // serialization between them.
        JobGraph.Builder job = JobGraph.named("chained");
        job.source("in", noopSource()).parallelism(4)
                .filter("drop-bots", value -> true).parallelism(4)
                .process("uppercase", upperCase()).parallelism(4)
                .sink("out", printSink()).parallelism(4);

        List<ChainGroup> chains = ChainBuilder.build(job.build());

        assertThat(chains).hasSize(1);
        assertThat(chains.getFirst().operatorIds())
                .containsExactly("in", "drop-bots", "uppercase", "out");
        assertThat(chains.getFirst().isChained()).isTrue();
    }

    @Test
    @DisplayName("condition 1: a hash exchange breaks the chain")
    void hashExchangeBreaksTheChain() {
        // A record may have to reach a different subtask than produced it, and a method call
        // cannot cross subtasks.
        JobGraph.Builder job = JobGraph.named("keyed");
        job.source("in", noopSource()).parallelism(4)
                .filter("drop-bots", value -> true).parallelism(4)
                .keyBy("by-value", value -> value)
                .process("sessions", keyedPassThrough()).parallelism(4)
                .sink("out", printSink()).parallelism(4);

        List<ChainGroup> chains = ChainBuilder.build(job.build());

        assertThat(chains).hasSize(2);
        assertThat(chains.get(0).operatorIds()).containsExactly("in", "drop-bots");
        assertThat(chains.get(1).operatorIds()).containsExactly("sessions", "out");
    }

    @Test
    @DisplayName("condition 2: unequal parallelism breaks the chain")
    void unequalParallelismBreaksTheChain() {
        // There is no subtask 2 or 3 on the receiving end for forward routing to reach. The
        // graph builder has already downgraded this edge to a rebalance.
        JobGraph.Builder job = JobGraph.named("narrowing");
        job.source("in", noopSource()).parallelism(4)
                .filter("drop-bots", value -> true).parallelism(4)
                .sink("out", printSink()).parallelism(2);

        List<ChainGroup> chains = ChainBuilder.build(job.build());

        assertThat(chains).hasSize(2);
        assertThat(chains.get(0).operatorIds()).containsExactly("in", "drop-bots");
        assertThat(chains.get(1).operatorIds()).containsExactly("out");
        assertThat(chains.get(1).parallelism()).isEqualTo(2);
    }

    @Test
    @DisplayName("condition 3: fan-out breaks the chain")
    void fanOutBreaksTheChain() {
        // An operator feeding two consumers has two continuations; a fused call chain has one.
        JobGraph.Builder job = JobGraph.named("fan-out");
        DataStream<String> in = job.source("in", noopSource()).parallelism(2);
        in.filter("left", value -> true).parallelism(2).sink("left-out", printSink())
                .parallelism(2);
        in.filter("right", value -> true).parallelism(2).sink("right-out", printSink())
                .parallelism(2);

        List<ChainGroup> chains = ChainBuilder.build(job.build());

        assertThat(chains).extracting(ChainGroup::operatorIds)
                .containsExactlyInAnyOrder(
                        List.of("in"),
                        List.of("left", "left-out"),
                        List.of("right", "right-out"));
    }

    @Test
    @DisplayName("a source chains with what follows it")
    void sourcesChainToo() {
        // Worth its own test: a record read from Kafka is filtered by the same thread that read
        // it, with no queue and no serialization between. In the LMS job this is the difference
        // between four threads and eight.
        JobGraph.Builder job = JobGraph.named("source-chain");
        job.source("clicks", noopSource()).parallelism(4)
                .filter("drop-bots", value -> true).parallelism(4)
                .sink("out", printSink()).parallelism(2);

        List<ChainGroup> chains = ChainBuilder.build(job.build());

        assertThat(chains.getFirst().operatorIds()).containsExactly("clicks", "drop-bots");
        assertThat(chains.getFirst().head().id()).isEqualTo("clicks");
    }

    @Test
    @DisplayName("every operator lands in exactly one group")
    void everyOperatorIsGroupedOnce() {
        JobGraph.Builder job = JobGraph.named("coverage");
        DataStream<String> in = job.source("in", noopSource()).parallelism(2);
        in.filter("a", value -> true).parallelism(2)
                .keyBy("by-value", value -> value)
                .process("b", keyedPassThrough()).parallelism(2)
                .sink("c", printSink()).parallelism(1);
        in.filter("d", value -> true).parallelism(2).sink("e", printSink()).parallelism(2);

        JobGraph graph = job.build();
        List<ChainGroup> chains = ChainBuilder.build(graph);

        List<String> grouped = chains.stream().flatMap(c -> c.operatorIds().stream()).toList();
        assertThat(grouped).hasSize(graph.operators().size());
        assertThat(grouped).doesNotHaveDuplicates();
        assertThat(grouped).containsExactlyInAnyOrderElementsOf(
                graph.operators().stream().map(op -> op.id()).toList());
    }

    // -------------------------------------------------------------------------------------

    private static Operator<String, String> upperCase() {
        return (record, out) -> out.collect(record.value().toUpperCase(java.util.Locale.ROOT));
    }

    private static Operator<String, Void> printSink() {
        return (record, out) -> {
        };
    }

    private static KeyedOperator<String, String, String> keyedPassThrough() {
        return new KeyedOperator<>() {
            @Override
            public void processElement(StreamRecord<String> record, Collector<String> out) {
                out.collect(record.value());
            }

            @Override
            public void onEventTimer(long timestamp, String key, Collector<String> out) {
            }
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
}
