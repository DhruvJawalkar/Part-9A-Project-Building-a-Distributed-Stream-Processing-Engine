package dev.dhruv.streaming.runtime;

import dev.dhruv.streaming.api.Collector;
import dev.dhruv.streaming.api.Operator;
import dev.dhruv.streaming.api.OperatorContext;
import dev.dhruv.streaming.api.StreamRecord;
import dev.dhruv.streaming.runtime.metrics.TaskMetricGroup;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that chained operators really are fused.
 *
 * <p>Phase 2's acceptance criterion is that a filter and a map at equal parallelism "run in one
 * thread with no serialization". Both halves of that are asserted here, and both are asserted by
 * observation rather than by inspecting configuration: the operators record which thread called
 * them, and the record's object identity is checked on the way through.
 */
class OperatorChainTest {

    @Test
    @DisplayName("every operator in a chain runs on the caller's thread")
    void chainRunsOnOneThread() throws Exception {
        Set<String> threadsSeen = ConcurrentHashMap.newKeySet();

        OperatorChain chain = new OperatorChain(List.of(
                new ThreadRecordingOperator(threadsSeen),
                new ThreadRecordingOperator(threadsSeen),
                new ThreadRecordingOperator(threadsSeen)));

        List<Object> collected = new ArrayList<>();
        chain.open(new NoOpContext());
        chain.processElement(new StreamRecord<>("value", 100L), collector(collected));

        assertThat(collected).containsExactly("value");
        assertThat(threadsSeen)
                .as("three fused operators must share the calling thread")
                .containsExactly(Thread.currentThread().getName());
    }

    @Test
    @DisplayName("a record passes through a chain without being copied")
    void chainDoesNotSerializeBetweenOperators() throws Exception {
        // The strongest available statement of "no serialization": the object the downstream
        // operator receives is the identical instance the upstream one emitted. A serialized
        // hop cannot preserve identity, so this failing would mean a copy happened somewhere.
        Payload payload = new Payload("catalog-7741");
        List<Object> received = new ArrayList<>();

        OperatorChain chain = new OperatorChain(List.of(
                passThrough(),
                passThrough(),
                (Operator<Object, Object>) (record, out) -> {
                    received.add(record.value());
                    out.collect(record.value());
                }));

        chain.open(new NoOpContext());
        chain.processElement(new StreamRecord<>(payload, 100L), collector(new ArrayList<>()));

        assertThat(received).hasSize(1);
        assertThat(received.getFirst()).isSameAs(payload);
    }

    @Test
    @DisplayName("an intermediate operator passes event time to the next one")
    void eventTimeSurvivesTheChain() throws Exception {
        // out.collect(value) deliberately takes no timestamp: a map that transforms a record
        // should not have to restate when the underlying event happened. So the chain has to
        // carry it, and this checks that the operator behind a pass-through sees the original
        // event time rather than zero.
        List<Long> seenByMiddle = new ArrayList<>();
        List<Long> seenByTail = new ArrayList<>();

        OperatorChain chain = new OperatorChain(List.of(
                passThrough(),
                recordTimestampInto(seenByMiddle),
                recordTimestampInto(seenByTail)));

        chain.open(new NoOpContext());
        chain.processElement(new StreamRecord<>("value", 1757836800000L),
                collector(new ArrayList<>()));

        assertThat(seenByMiddle).containsExactly(1757836800000L);
        assertThat(seenByTail).containsExactly(1757836800000L);
    }

    @Test
    @DisplayName("a filter mid-chain stops the record reaching what follows it")
    void filteringInAChainShortCircuits() throws Exception {
        List<Object> reachedTail = new ArrayList<>();

        OperatorChain chain = new OperatorChain(List.of(
                (Operator<Object, Object>) (record, out) -> {
                    if (!record.value().toString().startsWith("bot")) {
                        out.collect(record.value());
                    }
                },
                (Operator<Object, Object>) (record, out) -> {
                    reachedTail.add(record.value());
                    out.collect(record.value());
                }));

        chain.open(new NoOpContext());
        chain.processElement(new StreamRecord<>("bot-crawler", 1L), collector(new ArrayList<>()));
        chain.processElement(new StreamRecord<>("m-1001", 2L), collector(new ArrayList<>()));

        // The dropped record costs nothing downstream: no call, no allocation, no transport.
        assertThat(reachedTail).containsExactly("m-1001");
    }

    @Test
    @DisplayName("closes its operators in reverse order")
    void closesInReverse() throws Exception {
        List<String> closed = new ArrayList<>();

        OperatorChain chain = new OperatorChain(List.of(
                new ClosingOperator("first", closed),
                new ClosingOperator("second", closed),
                new ClosingOperator("third", closed)));

        chain.open(new NoOpContext());
        chain.close();

        // Reverse, so an operator flushing its last records on close still has somewhere to
        // send them.
        assertThat(closed).containsExactly("third", "second", "first");
    }

    // -------------------------------------------------------------------------------------

    private static Collector<Object> collector(List<Object> into) {
        return new Collector<>() {
            @Override
            public void collect(Object value) {
                into.add(value);
            }

            @Override
            public void collect(Object value, long timestamp) {
                into.add(value);
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static Operator<Object, Object> passThrough() {
        return (Operator<Object, Object>) (record, out) -> out.collect(record.value());
    }

    /** Records the event time it was handed, then passes the value along unchanged. */
    @SuppressWarnings("unchecked")
    private static Operator<Object, Object> recordTimestampInto(List<Long> into) {
        return (Operator<Object, Object>) (record, out) -> {
            into.add(record.timestamp());
            out.collect(record.value());
        };
    }

    /** A value with identity, so that "not copied" can be asserted with {@code isSameAs}. */
    private record Payload(String id) implements java.io.Serializable {
    }

    private static final class ThreadRecordingOperator implements Operator<Object, Object> {

        private static final long serialVersionUID = 1L;

        private final transient Set<String> threadsSeen;

        ThreadRecordingOperator(Set<String> threadsSeen) {
            this.threadsSeen = threadsSeen;
        }

        @Override
        public void processElement(StreamRecord<Object> record, Collector<Object> out) {
            threadsSeen.add(Thread.currentThread().getName());
            out.collect(record.value());
        }
    }

    private static final class ClosingOperator implements Operator<Object, Object> {

        private static final long serialVersionUID = 1L;

        private final String name;
        private final transient List<String> closed;

        ClosingOperator(String name, List<String> closed) {
            this.name = name;
            this.closed = closed;
        }

        @Override
        public void processElement(StreamRecord<Object> record, Collector<Object> out) {
            out.collect(record.value());
        }

        @Override
        public void close() {
            closed.add(name);
        }
    }

    /** Phase 2 operators need no state or timers, so the context only has to exist. */
    private static final class NoOpContext implements OperatorContext {

        private final TaskMetricGroup metrics = new TaskMetricGroup("test", 0);

        @Override
        public <T> dev.dhruv.streaming.api.state.ValueState<T> getValueState(
                String name, Class<T> type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> dev.dhruv.streaming.api.state.ListState<T> getListState(
                String name, Class<T> type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void registerEventTimer(long timestamp) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long currentWatermark() {
            return Long.MIN_VALUE;
        }

        @Override
        public dev.dhruv.streaming.api.metrics.MetricGroup metrics() {
            return metrics;
        }
    }
}
