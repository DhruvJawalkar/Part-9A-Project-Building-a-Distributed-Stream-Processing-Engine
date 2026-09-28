package dev.dhruv.streaming.connectors.file;

import dev.dhruv.streaming.api.Collector;
import dev.dhruv.streaming.api.SourceContext;
import dev.dhruv.streaming.api.metrics.Counter;
import dev.dhruv.streaming.api.metrics.Histogram;
import dev.dhruv.streaming.api.metrics.MetricGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the bounded source used by deterministic demos, without involving the runtime. */
class FileReplaySourceTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void replaysEveryJsonLineExactlyOnceAcrossDeterministicPartitions() throws Exception {
        Path fixture = writeFixture("""
                {"id":"a","eventTimeMillis":10}
                {"id":"b","eventTimeMillis":20}
                {"id":"c","eventTimeMillis":30}
                {"id":"d","eventTimeMillis":40}
                """);

        List<TestEvent> firstRun = replayAllSubtasks(fixture, 3);
        List<TestEvent> secondRun = replayAllSubtasks(fixture, 3);

        assertThat(firstRun).containsExactlyInAnyOrder(
                new TestEvent("a", 10), new TestEvent("b", 20),
                new TestEvent("c", 30), new TestEvent("d", 40));
        assertThat(secondRun).containsExactly(firstRun.toArray(TestEvent[]::new));
    }

    @Test
    void reportsEofAfterItsAssignedLinesHaveBeenEmitted() throws Exception {
        Path fixture = writeFixture("{" + "\"id\":\"only\",\"eventTimeMillis\":10}");
        FileReplaySource<TestEvent> source = FileReplaySource.of(fixture, TestEvent.class);
        List<TestEvent> replayed = new ArrayList<>();
        source.open(new TestSourceContext(0, 1));

        assertThat(source.poll(collectorFor(replayed))).isTrue();
        assertThat(source.poll(collectorFor(replayed))).isFalse();
        assertThat(replayed).containsExactly(new TestEvent("only", 10));

        source.close();
    }

    @Test
    void failsClearlyWhenAFixtureLineIsNotJson() throws Exception {
        Path fixture = writeFixture("not-json");
        FileReplaySource<TestEvent> source = FileReplaySource.of(fixture, TestEvent.class);
        source.open(new TestSourceContext(0, 1));

        assertThatThrownBy(() -> source.poll(discardingCollector()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("line 1");

        source.close();
    }

    private Path writeFixture(String contents) throws IOException {
        Path fixture = temporaryDirectory.resolve("events.jsonl");
        Files.writeString(fixture, contents);
        return fixture;
    }

    private static List<TestEvent> replayAllSubtasks(Path fixture, int parallelism) throws Exception {
        List<TestEvent> replayed = new ArrayList<>();
        for (int subtask = 0; subtask < parallelism; subtask++) {
            FileReplaySource<TestEvent> source = FileReplaySource.of(fixture, TestEvent.class);
            source.open(new TestSourceContext(subtask, parallelism));
            while (source.poll(collectorFor(replayed))) {
                // A source emits at most one matching line per poll, so EOF is unambiguous.
            }
            source.close();
        }
        return replayed;
    }

    private record TestEvent(String id, long eventTimeMillis) {
    }

    private static Collector<TestEvent> collectorFor(List<TestEvent> values) {
        return new Collector<>() {
            @Override
            public void collect(TestEvent value) {
                values.add(value);
            }

            @Override
            public void collect(TestEvent value, long timestamp) {
                values.add(value);
            }
        };
    }

    private static Collector<TestEvent> discardingCollector() {
        return new Collector<>() {
            @Override
            public void collect(TestEvent value) {
            }

            @Override
            public void collect(TestEvent value, long timestamp) {
            }
        };
    }

    private record TestSourceContext(int subtaskIndex, int parallelism) implements SourceContext {
        @Override
        public MetricGroup metrics() {
            return NoopMetricGroup.INSTANCE;
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
