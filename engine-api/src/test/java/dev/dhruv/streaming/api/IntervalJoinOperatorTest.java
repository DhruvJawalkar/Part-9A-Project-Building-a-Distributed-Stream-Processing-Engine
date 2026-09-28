package dev.dhruv.streaming.api;

import dev.dhruv.streaming.api.metrics.Counter;
import dev.dhruv.streaming.api.metrics.Histogram;
import dev.dhruv.streaming.api.metrics.MetricGroup;
import dev.dhruv.streaming.api.state.ListState;
import dev.dhruv.streaming.api.state.ValueState;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class IntervalJoinOperatorTest {

    @Test
    void emitsEachMatchOnceRegardlessOfWhichSideArrivesSecond() throws Exception {
        Harness firstLeft = new Harness(0, 30);
        firstLeft.record("member-item", Either.left("click"), 100);
        firstLeft.record("member-item", Either.right("borrow"), 130);

        Harness firstRight = new Harness(0, 30);
        firstRight.record("member-item", Either.right("borrow"), 130);
        firstRight.record("member-item", Either.left("click"), 100);

        assertThat(firstLeft.outputs).containsExactly("click/borrow");
        assertThat(firstRight.outputs).containsExactly("click/borrow");
    }

    @Test
    void includesBothIntervalEndpointsAndExcludesValuesOutsideThem() throws Exception {
        Harness harness = new Harness(10, 30);
        harness.record("key", Either.left("left"), 100);
        harness.record("key", Either.right("too-early"), 109);
        harness.record("key", Either.right("lower"), 110);
        harness.record("key", Either.right("upper"), 130);
        harness.record("key", Either.right("too-late"), 131);

        assertThat(harness.outputs).containsExactly("left/lower", "left/upper");
    }

    @Test
    void removesBothLmsSidesOnlyAfterTheStrictUpperBoundary() throws Exception {
        Harness harness = new Harness(0, 30);
        harness.record("key", Either.left("click"), 100);
        harness.record("key", Either.right("borrow"), 100);

        assertThat(harness.gauge("left-buffer-size")).isEqualTo(1);
        assertThat(harness.gauge("right-buffer-size")).isEqualTo(1);
        assertThat(harness.timers).containsExactlyInAnyOrder(131L, 131L);

        harness.fire("key", 131, 130);
        assertThat(harness.gauge("left-buffer-size")).isEqualTo(1);
        assertThat(harness.gauge("right-buffer-size")).isEqualTo(1);

        harness.fire("key", 131, 131);
        assertThat(harness.gauge("left-buffer-size")).isZero();
        assertThat(harness.gauge("right-buffer-size")).isZero();
    }

    @Test
    void retainsARightRecordLongEnoughForANegativeLowerBound() throws Exception {
        Harness harness = new Harness(-20, 10);
        harness.record("key", Either.right("right"), 100);

        // A right at 100 can still match a future left at 115: 100 - 115 == -15.
        harness.fire("key", 111, 111);
        harness.record("key", Either.left("left"), 115);

        assertThat(harness.outputs).containsExactly("left/right");
    }

    @Test
    void reconstructsGaugeAccountingWhenAKeyIsFirstTouchedAfterRestore() throws Exception {
        TestContext context = new TestContext();
        IntervalJoinOperator<String, String, String, String> beforeRestore = operator(0, 30);
        beforeRestore.open(context);
        context.currentKey = "key";
        beforeRestore.processElement(new StreamRecord<>(Either.left("click"), 100),
                collector(new ArrayList<>()));

        IntervalJoinOperator<String, String, String, String> restored = operator(0, 30);
        restored.open(context);
        context.currentKey = "key";
        List<String> outputs = new ArrayList<>();
        restored.processElement(new StreamRecord<>(Either.right("borrow"), 100), collector(outputs));

        assertThat(outputs).containsExactly("click/borrow");
        assertThat(context.metrics.gauge("left-buffer-size")).isEqualTo(1);
        assertThat(context.metrics.gauge("right-buffer-size")).isEqualTo(1);
    }

    @Test
    void sustainedArrivalsPlateauTheBufferWhenWatermarkTimersKeepCleaningUp() throws Exception {
        Harness harness = new Harness(0, 30);
        List<Long> observedSizes = new ArrayList<>();

        for (long timestamp = 0; timestamp <= 100; timestamp += 10) {
            harness.record("key", Either.left("click-" + timestamp), timestamp);
            if (timestamp >= 40) {
                // The record 40ms behind registered its timer at timestamp - 9 and is now stale.
                harness.fire("key", timestamp - 9, timestamp);
            }
            observedSizes.add(harness.gauge("left-buffer-size"));
        }

        assertThat(observedSizes).allMatch(size -> size <= 4);
        assertThat(observedSizes.getLast()).isEqualTo(4);
    }

    @Test
    void slowerStreamEffectInflatesTheFastSidesBufferUntilTheWatermarkCatchesUp()
            throws Exception {
        Harness harness = new Harness(0, 30);

        // The other input's watermark is stuck, so none of these otherwise-due left records
        // may be evicted. This is the slower-stream effect, made visible by the gauge.
        for (long timestamp = 0; timestamp < 100; timestamp += 10) {
            harness.record("key", Either.left("fast-click-" + timestamp), timestamp);
        }
        assertThat(harness.gauge("left-buffer-size")).isEqualTo(10);

        harness.fire("key", 31, 121);

        assertThat(harness.gauge("left-buffer-size")).isZero();
    }

    @Test
    void rejectsInvertedBounds() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> operator(31, 30))
                .withMessage("lowerBound must not exceed upperBound");
    }

    private static IntervalJoinOperator<String, String, String, String> operator(long lower, long upper) {
        return new IntervalJoinOperator<>(lower, upper, (left, right) -> left + "/" + right);
    }

    private static Collector<String> collector(List<String> outputs) {
        return new Collector<>() {
            @Override
            public void collect(String value) {
                outputs.add(value);
            }

            @Override
            public void collect(String value, long timestamp) {
                outputs.add(value);
            }
        };
    }

    private static final class Harness {
        private final TestContext context = new TestContext();
        private final IntervalJoinOperator<String, String, String, String> operator;
        private final List<String> outputs = new ArrayList<>();
        private final List<Long> timers = context.timers;

        private Harness(long lower, long upper) throws Exception {
            operator = operator(lower, upper);
            operator.open(context);
        }

        private void record(String key, Either<String, String> value, long timestamp) throws Exception {
            context.currentKey = key;
            operator.processElement(new StreamRecord<>(value, timestamp), collector(outputs));
        }

        private void fire(String key, long timer, long watermark) throws Exception {
            context.currentKey = key;
            context.watermark = watermark;
            operator.onEventTimer(timer, key, collector(outputs));
        }

        private long gauge(String name) {
            return context.metrics.gauge(name);
        }
    }

    private static final class TestContext implements OperatorContext {
        private final Map<String, Map<String, List<Object>>> lists = new HashMap<>();
        private final Map<String, Map<String, Object>> values = new HashMap<>();
        private final List<Long> timers = new ArrayList<>();
        private final TestMetricGroup metrics = new TestMetricGroup();
        private String currentKey;
        private long watermark = Long.MIN_VALUE;

        @Override
        public <T> ValueState<T> getValueState(String name, Class<T> type) {
            return new ValueState<>() {
                @Override
                public Optional<T> value() {
                    return Optional.ofNullable(values.computeIfAbsent(name, ignored -> new HashMap<>())
                            .get(currentKey)).map(type::cast);
                }

                @Override
                public void update(T value) {
                    values.computeIfAbsent(name, ignored -> new HashMap<>()).put(currentKey, value);
                }

                @Override
                public void clear() {
                    values.computeIfAbsent(name, ignored -> new HashMap<>()).remove(currentKey);
                }
            };
        }

        @Override
        public <T> ListState<T> getListState(String name, Class<T> type) {
            return new ListState<>() {
                @Override
                public Iterable<T> get() {
                    return lists.computeIfAbsent(name, ignored -> new HashMap<>())
                            .getOrDefault(currentKey, List.of()).stream().map(type::cast).toList();
                }

                @Override
                public void add(T value) {
                    lists.computeIfAbsent(name, ignored -> new HashMap<>())
                            .computeIfAbsent(currentKey, ignored -> new ArrayList<>()).add(value);
                }

                @Override
                public void update(List<T> replacement) {
                    Map<String, List<Object>> state = lists.computeIfAbsent(name, ignored -> new HashMap<>());
                    if (replacement.isEmpty()) {
                        state.remove(currentKey);
                    } else {
                        state.put(currentKey, new ArrayList<>(replacement));
                    }
                }

                @Override
                public void clear() {
                    lists.computeIfAbsent(name, ignored -> new HashMap<>()).remove(currentKey);
                }
            };
        }

        @Override
        public void registerEventTimer(long timestamp) {
            timers.add(timestamp);
        }

        @Override
        public long currentWatermark() {
            return watermark;
        }

        @Override
        public MetricGroup metrics() {
            return metrics;
        }
    }

    private static final class TestMetricGroup implements MetricGroup {
        private final Map<String, LongSupplier> gauges = new HashMap<>();

        @Override
        public Counter counter(String name) {
            return new Counter() {
                @Override public void increment() { }
                @Override public void increment(long amount) { }
                @Override public long count() { return 0; }
            };
        }

        @Override
        public Histogram histogram(String name) {
            return ignored -> { };
        }

        @Override
        public void gauge(String name, LongSupplier value) {
            gauges.put(name, value);
        }

        private long gauge(String name) {
            return gauges.get(name).getAsLong();
        }
    }
}
