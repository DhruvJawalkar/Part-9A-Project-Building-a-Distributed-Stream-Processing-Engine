package dev.dhruv.streaming.runtime;

import dev.dhruv.streaming.api.Collector;
import dev.dhruv.streaming.api.JobExecutor;
import dev.dhruv.streaming.api.Operator;
import dev.dhruv.streaming.api.OperatorContext;
import dev.dhruv.streaming.api.Source;
import dev.dhruv.streaming.api.SourceContext;
import dev.dhruv.streaming.api.StreamRecord;
import dev.dhruv.streaming.api.graph.DataStream;
import dev.dhruv.streaming.api.graph.JobGraph;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.time.Duration;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end tests for running a job in one JVM.
 *
 * <p>Every job here reads a bounded source, so the pipeline terminates on its own and the test
 * can simply wait for it rather than sleeping and hoping. That is also why
 * {@link dev.dhruv.streaming.api.Watermark#MAX} exists: a bounded source announces that it is
 * finished, and the announcement propagates.
 *
 * <p>Results are collected through static fields rather than instance ones. That is not
 * laziness -- operators are serialized on their way into a subtask, so an instance field would
 * be a copy per subtask and a test could not read it back.
 */
class LocalJobExecutorTest {

    private static final Map<String, Collection<String>> COLLECTED = new ConcurrentHashMap<>();
    private static final Map<String, Set<Integer>> INSTANCE_IDENTITIES = new ConcurrentHashMap<>();

    @BeforeEach
    void clearStatics() {
        COLLECTED.clear();
        INSTANCE_IDENTITIES.clear();
    }

    @Test
    @Timeout(30)
    @DisplayName("runs a source through a filter into a sink")
    void linearPipeline() throws Exception {
        List<String> input = List.of("keep-1", "drop-1", "keep-2", "drop-2", "keep-3");

        JobGraph.Builder job = JobGraph.named("linear");
        job.source("in", new ListSource(input))
                .filter("only-keeps", value -> value.startsWith("keep"))
                .sink("out", new CollectingSink("linear"));

        runToCompletion(job.build());

        assertThat(COLLECTED.get("linear"))
                .containsExactlyInAnyOrder("keep-1", "keep-2", "keep-3");
    }

    @Test
    @Timeout(30)
    @DisplayName("every record survives a pipeline running four ways")
    void parallelPipelineLosesNothing() throws Exception {
        List<String> input = IntStream.range(0, 400).mapToObj(i -> "v" + i).toList();

        JobGraph.Builder job = JobGraph.named("parallel");
        job.source("in", new ListSource(input)).parallelism(4)
                .filter("pass", value -> true).parallelism(4)
                .sink("out", new CollectingSink("parallel")).parallelism(4);

        runToCompletion(job.build());

        assertThat(COLLECTED.get("parallel")).containsExactlyInAnyOrderElementsOf(input);
    }

    @Test
    @Timeout(30)
    @DisplayName("a narrower sink still receives every record, via rebalance")
    void rebalanceIntoNarrowerSink() throws Exception {
        List<String> input = IntStream.range(0, 200).mapToObj(i -> "v" + i).toList();

        // Four upstream subtasks feeding two sinks. The graph builder turns this forward edge
        // into a rebalance, because there is no subtask 2 or 3 on the receiving end.
        JobGraph.Builder job = JobGraph.named("narrowing");
        job.source("in", new ListSource(input)).parallelism(4)
                .filter("pass", value -> true).parallelism(4)
                .sink("out", new CollectingSink("narrowing")).parallelism(2);

        runToCompletion(job.build());

        assertThat(COLLECTED.get("narrowing")).containsExactlyInAnyOrderElementsOf(input);
    }

    @Test
    @Timeout(30)
    @DisplayName("each subtask gets its own operator instance")
    void eachSubtaskGetsItsOwnInstance() throws Exception {
        // Regression test for a real bug. The job graph holds one operator object; running it
        // four ways must not hand the same object to four threads. When it did, four source
        // subtasks shared one KafkaConsumer and three of them died on first use.
        JobGraph.Builder job = JobGraph.named("instances");
        job.source("in", new ListSource(List.of("a", "b", "c", "d"))).parallelism(4)
                .process("identify", new IdentityRecordingOperator("instances")).parallelism(4)
                .sink("out", new CollectingSink("instances")).parallelism(4);

        runToCompletion(job.build());

        assertThat(INSTANCE_IDENTITIES.get("instances"))
                .as("four subtasks must be four distinct operator objects")
                .hasSize(4);
    }

    @Test
    @Timeout(30)
    @DisplayName("reports per-subtask record counts")
    void metricsAreScopedPerSubtask() throws Exception {
        List<String> input = IntStream.range(0, 100).mapToObj(i -> "v" + i).toList();

        JobGraph.Builder job = JobGraph.named("metrics");
        job.source("in", new ListSource(input)).parallelism(2)
                .filter("pass", value -> true).parallelism(2)
                .sink("out", new CollectingSink("metrics")).parallelism(2);

        Map<String, Map<String, Long>> metrics = runToCompletion(job.build());

        assertThat(metrics).containsKeys("in(1/2)", "in(2/2)", "pass(1/2)", "pass(2/2)");
        long filtered = metrics.get("pass(1/2)").get("records-in")
                + metrics.get("pass(2/2)").get("records-in");
        assertThat(filtered).isEqualTo(100);
    }

    @Test
    @DisplayName("routes a hash exchange through the keyed task")
    void hashExchangeRoutesToKeyedTask() throws Exception {
        JobGraph.Builder job = JobGraph.named("keyed");
        job.source("in", new ListSource(List.of("a", "b", "a", "c"))).parallelism(2)
                .keyBy("by-value", value -> value)
                .process("keyed-op", new PassThroughKeyedOperator()).parallelism(2)
                .sink("out", new CollectingSink("keyed")).parallelism(2);

        runToCompletion(job.build());

        assertThat(COLLECTED.get("keyed")).containsExactlyInAnyOrder("a", "b", "a", "c");
    }

    @Test
    @Timeout(10)
    @DisplayName("fans one upstream stream out to independently routed branches")
    void fanOutDeliversEachRecordToEveryBranch() throws Exception {
        JobGraph.Builder job = JobGraph.named("fan-out");
        DataStream<String> input = job.source("in", new ListSource(List.of("a", "b", "c")));
        input.<String>process("left", (record, out) -> out.collect("left-" + record.value()))
                .sink("left-out", new CollectingSink("fan-out-left"));
        input.<String>process("right", (record, out) -> out.collect("right-" + record.value()))
                .sink("right-out", new CollectingSink("fan-out-right"));

        runToCompletion(job.build());

        assertThat(COLLECTED.get("fan-out-left"))
                .containsExactlyInAnyOrder("left-a", "left-b", "left-c");
        assertThat(COLLECTED.get("fan-out-right"))
                .containsExactlyInAnyOrder("right-a", "right-b", "right-c");
    }

    @Test
    @Timeout(10)
    @DisplayName("two union inputs use distinct channels and finish together")
    void multiInputSendersDoNotAliasInputChannelZero() throws Exception {
        JobGraph.Builder job = JobGraph.named("multi-input-channels");
        DataStream<String> left = job.source("left-in", new ListSource(List.of("left")));
        DataStream<String> right = job.source("right-in", new ListSource(List.of("right")));
        left.union("union", right).sink("out", new CollectingSink("multi-input-channels"));

        // A bounded source propagates Watermark.MAX. Completion here proves that both union
        // input channels received it; an aliased channel leaves the unused one open forever.
        runToCompletion(job.build());

        assertThat(COLLECTED.get("multi-input-channels")).containsExactlyInAnyOrder(
                "left", "right");
    }

    @Test
    @Timeout(10)
    @DisplayName("a silent source channel no longer stalls event-time progress after idleness")
    void idlenessUnblocksTheLocalExecutorWatermarkMinimum() throws Exception {
        assertThat(runWithOptionalIdleness(Duration.ZERO)).isFalse();
        assertThat(runWithOptionalIdleness(Duration.ofMillis(10))).isTrue();
    }

    @Test
    @DisplayName("rejects an operator that cannot be serialized")
    void unserializableOperatorIsRejected() {
        // The check that earns its place: a field holding a resource rather than configuration
        // fails here, at startup in one JVM, instead of on deployment to a worker in Phase 2.
        JobGraph.Builder job = JobGraph.named("unserializable");
        job.source("in", new ListSource(List.of("a")))
                .process("bad", new HoldsSomethingUnserializable())
                .sink("out", new CollectingSink("unserializable"));

        JobExecutor executor = new LocalJobExecutor(job.build());

        assertThatThrownBy(executor::start)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("could not be serialized");
    }

    // -------------------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------------------

    private static Map<String, Map<String, Long>> runToCompletion(JobGraph graph)
            throws InterruptedException {
        LocalJobExecutor executor = new LocalJobExecutor(graph);
        try {
            executor.start();
            executor.awaitTermination();
            return executor.metrics();
        } finally {
            executor.close();
        }
    }

    private static boolean runWithOptionalIdleness(Duration idleTimeout) throws Exception {
        CountDownLatch watermarkObserved = WatermarkObservingOperator.newLatch();
        JobGraph.Builder job = JobGraph.named("idleness-" + idleTimeout.toMillis());
        job.source("in", new OneSilentPartitionSource()).parallelism(2)
                .withEventTime(value -> value.timestamp(), Duration.ZERO)
                .withIdleness(idleTimeout)
                // A narrower operator forces the two source subtasks into distinct input
                // channels, which is where the watermark minimum and idleness matter.
                .process("observe", new WatermarkObservingOperator()).parallelism(1)
                .sink("out", (record, out) -> { }).parallelism(1);

        LocalJobExecutor executor = new LocalJobExecutor(job.build());
        try {
            executor.start();
            return watermarkObserved.await(500, TimeUnit.MILLISECONDS);
        } finally {
            executor.close();
        }
    }

    /**
     * A bounded source that replays a fixed list, split across its subtasks.
     *
     * <p>Each subtask takes the elements whose position is congruent to its own index, which is
     * the same shape as a Kafka source taking a slice of the partitions. Returning false ends
     * the stream, which is what lets these tests terminate rather than time out.
     */
    private static final class ListSource implements Source<String> {

        private static final long serialVersionUID = 1L;

        private final List<String> values;
        private transient List<String> mine;

        ListSource(List<String> values) {
            this.values = values;
        }

        @Override
        public void open(SourceContext context) {
            mine = new ArrayList<>();
            for (int i = 0; i < values.size(); i++) {
                if (i % context.parallelism() == context.subtaskIndex()) {
                    mine.add(values.get(i));
                }
            }
        }

        @Override
        public boolean poll(Collector<String> out) {
            mine.forEach(out::collect);
            mine = List.of();
            return false;
        }

        @Override
        public void close() {
        }
    }

    /** A sink that records what it received into a static keyed by test name. */
    private static final class CollectingSink implements Operator<String, Void> {

        private static final long serialVersionUID = 1L;

        private final String key;

        CollectingSink(String key) {
            this.key = key;
        }

        @Override
        public void processElement(StreamRecord<String> record, Collector<Void> out) {
            COLLECTED.computeIfAbsent(key, ignored -> new ConcurrentLinkedQueue<>())
                    .add(record.value());
        }
    }

    /** Records the identity of the instance that ran, to prove subtasks do not share one. */
    private static final class IdentityRecordingOperator implements Operator<String, String> {

        private static final long serialVersionUID = 1L;

        private final String key;

        IdentityRecordingOperator(String key) {
            this.key = key;
        }

        @Override
        public void open(OperatorContext ctx) {
            INSTANCE_IDENTITIES.computeIfAbsent(key, ignored -> ConcurrentHashMap.newKeySet())
                    .add(System.identityHashCode(this));
        }

        @Override
        public void processElement(StreamRecord<String> record, Collector<String> out) {
            out.collect(record.value());
        }
    }

    private static final class PassThroughKeyedOperator
            implements dev.dhruv.streaming.api.KeyedOperator<String, String, String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void processElement(StreamRecord<String> record, Collector<String> out) {
            out.collect(record.value());
        }

        @Override
        public void onEventTimer(long timestamp, String key, Collector<String> out) {
        }
    }

    private record TimedValue(String value, long timestamp) implements java.io.Serializable {
    }

    /** Subtask zero advances event time; subtask one stays alive but produces no events. */
    private static final class OneSilentPartitionSource implements Source<TimedValue> {
        private transient int subtask;
        private transient int poll;

        @Override
        public void open(SourceContext context) {
            subtask = context.subtaskIndex();
        }

        @Override
        public boolean poll(Collector<TimedValue> out) throws InterruptedException {
            if (subtask == 1) {
                return true;
            }
            if (poll++ == 0) {
                out.collect(new TimedValue("first", 100), 100);
                return true;
            }
            Thread.sleep(60);
            out.collect(new TimedValue("advance", 1_000), 1_000);
            return false;
        }

        @Override
        public void close() {
        }
    }

    private static final class WatermarkObservingOperator implements Operator<TimedValue, TimedValue> {
        private static volatile CountDownLatch observed;

        static CountDownLatch newLatch() {
            observed = new CountDownLatch(1);
            return observed;
        }

        @Override
        public void processElement(StreamRecord<TimedValue> record, Collector<TimedValue> out) {
        }

        @Override
        public void onWatermark(long watermark, Collector<TimedValue> out) {
            if (watermark >= 1_000) {
                observed.countDown();
            }
        }
    }

    /** Holds a non-serializable field, which is the mistake {@link TaskInstances} catches. */
    private static final class HoldsSomethingUnserializable implements Operator<String, String> {

        private static final long serialVersionUID = 1L;

        @SuppressWarnings("unused")
        private final Object resource = new Object();

        @Override
        public void processElement(StreamRecord<String> record, Collector<String> out) {
            out.collect(record.value());
        }
    }
}
