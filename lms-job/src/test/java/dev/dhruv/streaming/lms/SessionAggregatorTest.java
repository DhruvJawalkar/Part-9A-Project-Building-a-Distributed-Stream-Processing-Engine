package dev.dhruv.streaming.lms;

import dev.dhruv.streaming.api.Collector;
import dev.dhruv.streaming.api.OperatorContext;
import dev.dhruv.streaming.api.StreamRecord;
import dev.dhruv.streaming.api.metrics.Counter;
import dev.dhruv.streaming.api.metrics.Histogram;
import dev.dhruv.streaming.api.metrics.MetricGroup;
import dev.dhruv.streaming.api.state.ListState;
import dev.dhruv.streaming.api.state.ValueState;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.LongSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/** Behavioural tests for the job's incremental event-time session operator. */
class SessionAggregatorTest {

    private static final long GAP_MILLIS = SessionAggregator.SESSION_GAP.toMillis();

    @Test
    void extendingASessionLeavesTheOldTimerHarmlessThenClearsStateWhenTheNewOneFires()
            throws Exception {
        TestOperatorContext context = new TestOperatorContext();
        SessionAggregator aggregator = open(context);
        RecordingCollector output = new RecordingCollector();
        context.setCurrentKey("m-1001");

        aggregator.processElement(record("m-1001", "distributed systems", 1_000), output);
        aggregator.processElement(record("m-1001", "event time", 31_000), output);

        assertThat(context.timers()).containsExactly(1_000 + GAP_MILLIS, 31_000 + GAP_MILLIS);

        aggregator.onEventTimer(1_000 + GAP_MILLIS, "m-1001", output);
        assertThat(output.rows).isEmpty();
        assertThat(context.entryCount()).as("the later session end is still live").isEqualTo(2);

        aggregator.onEventTimer(31_000 + GAP_MILLIS, "m-1001", output);
        assertThat(output.rows).containsExactly(new StreamRecord<>(new SessionRow(
                "m-1001", 1_000, 31_000, 31_000 + GAP_MILLIS, 30_000, 2,
                List.of("distributed systems", "event time")), 31_000 + GAP_MILLIS));
        assertThat(context.entryCount()).as("closed member state must not leak").isZero();
    }

    @Test
    void outOfOrderEventsStillProduceTheCorrectStartAndLatestEnd() throws Exception {
        TestOperatorContext context = new TestOperatorContext();
        SessionAggregator aggregator = open(context);
        RecordingCollector output = new RecordingCollector();
        context.setCurrentKey("m-1002");

        aggregator.processElement(record("m-1002", "later", 20_000), output);
        aggregator.processElement(record("m-1002", "earlier", 10_000), output);

        assertThat(context.timers()).containsExactly(20_000 + GAP_MILLIS);
        aggregator.onEventTimer(20_000 + GAP_MILLIS, "m-1002", output);

        assertThat(output.rows).singleElement().extracting(StreamRecord::value)
                .isEqualTo(new SessionRow("m-1002", 10_000, 20_000,
                        20_000 + GAP_MILLIS, 10_000, 2, List.of("earlier", "later")));
    }

    @Test
    void keepsOnlyIncrementalStateWhileASessionIsOpen() throws Exception {
        TestOperatorContext context = new TestOperatorContext();
        SessionAggregator aggregator = open(context);
        RecordingCollector output = new RecordingCollector();
        context.setCurrentKey("m-1003");

        for (int i = 0; i < 100; i++) {
            aggregator.processElement(record("m-1003", "same search", 1_000L + i), output);
        }

        assertThat(context.entryCount())
                .as("one accumulator and one end timestamp, not one state entry per event")
                .isEqualTo(2);
    }

    @Test
    void gapSeparatedEventClosesThePriorSessionAndStartsANewOne() throws Exception {
        TestOperatorContext context = new TestOperatorContext();
        SessionAggregator aggregator = open(context);
        RecordingCollector output = new RecordingCollector();
        context.setCurrentKey("m-1004");

        long first = 1_000L;
        long second = first + GAP_MILLIS + 1;
        aggregator.processElement(record("m-1004", "first", first), output);
        aggregator.processElement(record("m-1004", "second", second), output);

        assertThat(output.rows).containsExactly(new StreamRecord<>(new SessionRow(
                "m-1004", first, first, first + GAP_MILLIS, 0, 1, List.of("first")),
                first + GAP_MILLIS));
        assertThat(context.entryCount()).isEqualTo(2);

        // The first timer is stale after the eager close; the second owns the new session.
        aggregator.onEventTimer(first + GAP_MILLIS, "m-1004", output);
        aggregator.onEventTimer(second + GAP_MILLIS, "m-1004", output);

        assertThat(output.rows).containsExactly(
                new StreamRecord<>(new SessionRow("m-1004", first, first, first + GAP_MILLIS,
                        0, 1, List.of("first")), first + GAP_MILLIS),
                new StreamRecord<>(new SessionRow("m-1004", second, second,
                        second + GAP_MILLIS, 0, 1, List.of("second")), second + GAP_MILLIS));
        assertThat(context.entryCount()).isZero();
    }

    @Test
    void dropsAnEventTooOldToMergeWithTheOpenSession() throws Exception {
        TestOperatorContext context = new TestOperatorContext();
        SessionAggregator aggregator = open(context);
        RecordingCollector output = new RecordingCollector();
        context.setCurrentKey("m-1005");

        long first = GAP_MILLIS + 1_000L;
        aggregator.processElement(record("m-1005", "current", first), output);
        aggregator.processElement(record("m-1005", "late", 999L), output);

        assertThat(context.lateSessionEvents()).isEqualTo(1);
        aggregator.onEventTimer(first + GAP_MILLIS, "m-1005", output);
        assertThat(output.rows).containsExactly(new StreamRecord<>(new SessionRow(
                "m-1005", first, first, first + GAP_MILLIS, 0, 1, List.of("current")),
                first + GAP_MILLIS));
        assertThat(context.entryCount()).isZero();
    }

    @Test
    void dropsAnEventBehindTheWatermarkByDefaultButAcceptsItWithinConfiguredLateness()
            throws Exception {
        Properties fixture = fixture("late-event.properties");
        String member = fixture.getProperty("member");
        long current = Long.parseLong(fixture.getProperty("current.event.time"));
        long late = Long.parseLong(fixture.getProperty("late.event.time"));
        long watermark = Long.parseLong(fixture.getProperty("watermark"));
        Duration allowedLateness = Duration.ofMillis(Long.parseLong(
                fixture.getProperty("allowed.lateness")));

        TestOperatorContext droppingContext = new TestOperatorContext();
        droppingContext.setCurrentKey(member);
        SessionAggregator dropping = open(droppingContext);
        RecordingCollector droppedOutput = new RecordingCollector();
        dropping.processElement(record(member, "current", current), droppedOutput);
        droppingContext.setCurrentWatermark(watermark);
        dropping.processElement(record(member, "late", late), droppedOutput);
        dropping.onEventTimer(current + GAP_MILLIS, member, droppedOutput);

        assertThat(droppingContext.lateSessionEvents()).isEqualTo(1);
        assertThat(droppingContext.acceptedLateSessionEvents()).isZero();
        assertThat(droppedOutput.rows).singleElement().extracting(StreamRecord::value)
                .isEqualTo(new SessionRow(member, current, current,
                        current + GAP_MILLIS, 0, 1, List.of("current")));

        TestOperatorContext acceptingContext = new TestOperatorContext();
        acceptingContext.setCurrentKey(member);
        SessionAggregator accepting = new SessionAggregator(allowedLateness);
        accepting.open(acceptingContext);
        RecordingCollector updatedOutput = new RecordingCollector();
        accepting.processElement(record(member, "current", current), updatedOutput);
        acceptingContext.setCurrentWatermark(watermark);
        accepting.processElement(record(member, "late", late), updatedOutput);
        accepting.onEventTimer(current + GAP_MILLIS, member, updatedOutput);

        assertThat(acceptingContext.lateSessionEvents()).isZero();
        assertThat(acceptingContext.acceptedLateSessionEvents()).isEqualTo(1);
        assertThat(updatedOutput.rows).singleElement().extracting(StreamRecord::value)
                .isEqualTo(new SessionRow(member, late, current,
                        current + GAP_MILLIS, current, 2, List.of("current", "late")));
    }

    private static Properties fixture(String name) throws IOException {
        Path workingDirectory = Path.of(System.getProperty("user.dir"));
        Path path = workingDirectory.resolve(Path.of("demos", "fixtures", name));
        if (!Files.exists(path)) {
            path = workingDirectory.resolve(Path.of("..", "demos", "fixtures", name)).normalize();
        }
        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(path)) {
            properties.load(reader);
        }
        return properties;
    }

    private static SessionAggregator open(TestOperatorContext context) throws Exception {
        SessionAggregator aggregator = new SessionAggregator();
        aggregator.open(context);
        return aggregator;
    }

    private static StreamRecord<ClickEvent> record(String memberId, String searchTerm, long timestamp) {
        return new StreamRecord<>(new ClickEvent(memberId, "catalog-1", searchTerm,
                ClickEvent.RESULT_CLICK, timestamp), timestamp);
    }

    private static final class RecordingCollector implements Collector<SessionRow> {
        private final List<StreamRecord<SessionRow>> rows = new ArrayList<>();

        @Override
        public void collect(SessionRow value) {
            rows.add(new StreamRecord<>(value, Long.MIN_VALUE));
        }

        @Override
        public void collect(SessionRow value, long timestamp) {
            rows.add(new StreamRecord<>(value, timestamp));
        }
    }

    /** A tiny keyed state fake keeps this test on the API side of the module boundary. */
    private static final class TestOperatorContext implements OperatorContext {
        private final Map<String, Map<Object, Object>> valuesByState = new HashMap<>();
        private final List<Long> timers = new ArrayList<>();
        private Object currentKey;
        private long lateSessionEvents;
        private long acceptedLateSessionEvents;
        private long currentWatermark = Long.MIN_VALUE;

        void setCurrentKey(Object currentKey) {
            this.currentKey = currentKey;
        }

        List<Long> timers() {
            return List.copyOf(timers);
        }

        int entryCount() {
            return valuesByState.values().stream().mapToInt(Map::size).sum();
        }

        long lateSessionEvents() {
            return lateSessionEvents;
        }

        long acceptedLateSessionEvents() {
            return acceptedLateSessionEvents;
        }

        void setCurrentWatermark(long currentWatermark) {
            this.currentWatermark = currentWatermark;
        }

        @Override
        public <T> ValueState<T> getValueState(String name, Class<T> type) {
            Map<Object, Object> values = valuesByState.computeIfAbsent(name, ignored -> new HashMap<>());
            return new ValueState<>() {
                @Override
                public Optional<T> value() {
                    return Optional.ofNullable(type.cast(values.get(currentKey)));
                }

                @Override
                public void update(T value) {
                    values.put(currentKey, value);
                }

                @Override
                public void clear() {
                    values.remove(currentKey);
                }
            };
        }

        @Override
        public <T> ListState<T> getListState(String name, Class<T> type) {
            throw new UnsupportedOperationException("SessionAggregator only needs value state");
        }

        @Override
        public void registerEventTimer(long timestamp) {
            timers.add(timestamp);
        }

        @Override
        public long currentWatermark() {
            return currentWatermark;
        }

        @Override
        public MetricGroup metrics() {
            return new MetricGroup() {
                @Override
                public Counter counter(String name) {
                    if ("late-session-events".equals(name)) {
                        return new Counter() {
                            @Override
                            public void increment() {
                                lateSessionEvents++;
                            }

                            @Override
                            public void increment(long amount) {
                                lateSessionEvents += amount;
                            }

                            @Override
                            public long count() {
                                return lateSessionEvents;
                            }
                        };
                    }
                    if ("accepted-late-session-events".equals(name)) {
                        return new Counter() {
                            @Override
                            public void increment() {
                                acceptedLateSessionEvents++;
                            }

                            @Override
                            public void increment(long amount) {
                                acceptedLateSessionEvents += amount;
                            }

                            @Override
                            public long count() {
                                return acceptedLateSessionEvents;
                            }
                        };
                    }
                    return NoopCounter.INSTANCE;
                }

                @Override
                public Histogram histogram(String name) {
                    return value -> { };
                }

                @Override
                public void gauge(String name, LongSupplier value) {
                }
            };
        }
    }

    private enum NoopMetricGroup implements MetricGroup {
        INSTANCE;

        @Override
        public Counter counter(String name) {
            return NoopCounter.INSTANCE;
        }

        @Override
        public Histogram histogram(String name) {
            return value -> { };
        }

        @Override
        public void gauge(String name, LongSupplier value) {
        }
    }

    private enum NoopCounter implements Counter {
        INSTANCE;

        @Override
        public void increment() {
        }

        @Override
        public void increment(long amount) {
        }

        @Override
        public long count() {
            return 0;
        }
    }
}
