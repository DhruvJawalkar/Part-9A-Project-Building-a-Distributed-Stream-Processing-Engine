package dev.dhruv.streaming.lms;

import dev.dhruv.streaming.api.Collector;
import dev.dhruv.streaming.api.Operator;
import dev.dhruv.streaming.api.Source;
import dev.dhruv.streaming.api.SourceContext;
import dev.dhruv.streaming.api.StreamRecord;
import dev.dhruv.streaming.api.graph.JobGraph;
import dev.dhruv.streaming.runtime.LocalJobExecutor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.assertj.core.api.Assertions.assertThat;

/** Demo 3 through real source, transport, watermark, keyed state, timer and sink task loops. */
class LateEventPipelineAcceptanceTest {
    private static final ConcurrentLinkedQueue<SessionRow> ROWS = new ConcurrentLinkedQueue<>();

    @Test
    @Timeout(15)
    void replayDropsOrRevisesTheOpenSessionBehindAnActualWatermark() throws Exception {
        Path fixture = Path.of("../demos/fixtures/late-event.properties").toAbsolutePath().normalize();
        if (!Files.exists(fixture)) {
            fixture = Path.of("demos/fixtures/late-event.properties");
        }
        Properties values = new Properties();
        try (Reader reader = Files.newBufferedReader(fixture)) {
            values.load(reader);
        }
        String member = values.getProperty("member");
        long current = Long.parseLong(values.getProperty("current.event.time"));
        long late = Long.parseLong(values.getProperty("late.event.time"));
        long watermark = Long.parseLong(values.getProperty("watermark"));
        long allowance = Long.parseLong(values.getProperty("allowed.lateness"));
        Result dropped = replay(member, current, late, current - watermark, Duration.ZERO);
        Result revised = replay(member, current, late, current - watermark,
                Duration.ofMillis(allowance));
        long end = current + SessionAggregator.SESSION_GAP.toMillis();
        assertThat(dropped.rows()).containsExactly(new SessionRow(member, current, current, end,
                0, 1, List.of("current")));
        assertThat(revised.rows()).containsExactly(new SessionRow(member, late, current, end,
                current - late, 2, List.of("current", "late")));
        assertThat(dropped.metrics().get("late-session-events")).isEqualTo(1);
        assertThat(dropped.metrics().get("accepted-late-session-events")).isZero();
        assertThat(revised.metrics().get("late-session-events")).isZero();
        assertThat(revised.metrics().get("accepted-late-session-events")).isEqualTo(1);
        Path report = Path.of("build/demo-3/report.txt");
        Files.createDirectories(report.getParent());
        String evidence = "Actual source watermark=" + watermark + "\nDefault: " + dropped
                + "\nAllowed lateness " + allowance + "ms: " + revised + "\n";
        Files.writeString(report, evidence);
        System.out.print(evidence);
    }

    private static Result replay(String member, long current, long late, long disorder,
                                 Duration allowedLateness) throws Exception {
        ROWS.clear();
        JobGraph.Builder job = JobGraph.named("late-event-demo");
        job.source("clicks", new LateReplay(member, current, late))
                .withEventTime(ClickEvent::eventTimeMillis, Duration.ofMillis(disorder))
                .parallelism(1)
                .keyBy("member", ClickEvent::memberId)
                .process("sessions", new SessionAggregator(allowedLateness))
                .parallelism(1)
                .sink("rows", new CapturingSink()).parallelism(1);
        try (LocalJobExecutor executor = new LocalJobExecutor(job.build())) {
            executor.start();
            executor.awaitTermination();
            Map<String, Long> metrics = executor.metrics().get("sessions(1/1)");
            assertThat(metrics).as("the local executor's session task metrics").isNotNull();
            return new Result(List.copyOf(ROWS), metrics);
        }
    }

    private record Result(List<SessionRow> rows, Map<String, Long> metrics) { }

    private static final class LateReplay implements Source<ClickEvent> {
        private final String member;
        private final long current;
        private final long late;
        private transient int poll;

        private LateReplay(String member, long current, long late) {
            this.member = member;
            this.current = current;
            this.late = late;
        }

        @Override
        public void open(SourceContext context) { }

        @Override
        public boolean poll(Collector<ClickEvent> out) {
            // Returning after the first record lets the real SourceTask emit its watermark
            // before the second poll. FIFO channels preserve that ordering downstream.
            boolean first = poll++ == 0;
            out.collect(new ClickEvent(member, "catalog-1", first ? "current" : "late",
                    ClickEvent.RESULT_CLICK, first ? current : late));
            return first;
        }

        @Override
        public void close() { }
    }

    private static final class CapturingSink implements Operator<SessionRow, Void> {
        @Override
        public void processElement(StreamRecord<SessionRow> record, Collector<Void> out) {
            ROWS.add(record.value());
        }
    }
}
