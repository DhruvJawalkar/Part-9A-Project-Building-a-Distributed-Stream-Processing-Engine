package dev.dhruv.streaming.lms;

import dev.dhruv.streaming.api.Collector;
import dev.dhruv.streaming.api.JobExecutor;
import dev.dhruv.streaming.api.JobExecutors;
import dev.dhruv.streaming.api.Operator;
import dev.dhruv.streaming.api.StreamRecord;
import dev.dhruv.streaming.api.graph.DataStream;
import dev.dhruv.streaming.api.graph.JobGraph;
import dev.dhruv.streaming.connectors.file.FileReplaySource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Acceptance proof for the event-time session path as a job author uses it.
 *
 * <p>This intentionally reaches the runtime only through {@link JobExecutors}. The source's
 * fixed, out-of-order JSON Lines fixture and the sink's canonical row encoding mean the two
 * executions must produce the same bytes, independent of replay speed or wall-clock time.
 */
class SessionPipelineAcceptanceTest {

    private static final Duration OUT_OF_ORDERNESS = Duration.ofSeconds(5);
    private static final ConcurrentLinkedQueue<byte[]> CAPTURED_ROWS = new ConcurrentLinkedQueue<>();

    @Test
    @Timeout(10)
    void replaysOutOfOrderClicksIntoByteIdenticalClosedSessions() throws Exception {
        List<byte[]> firstRun = runFixture();
        List<byte[]> secondRun = runFixture();

        assertThat(firstRun).hasSize(3);
        assertThat(secondRun).hasSameSizeAs(firstRun);
        for (int index = 0; index < firstRun.size(); index++) {
            assertThat(secondRun.get(index)).containsExactly(firstRun.get(index));
        }

        assertThat(rows(firstRun)).containsExactly(
                "member-alpha|18000|20000|920000|2000|2|earlier,later",
                "member-beta|1000000|1000000|1900000|0|1|concurrency",
                // The late alpha event is a new session. This makes the state clear performed
                // by the first session's event-time timer observable at the job boundary.
                "member-alpha|2000000|2000000|2900000|0|1|new session");
    }

    private static List<byte[]> runFixture() throws Exception {
        CAPTURED_ROWS.clear();
        try (JobExecutor executor = JobExecutors.local(sessionJob(fixturePath()))) {
            executor.start();
            executor.awaitTermination();
        }
        return List.copyOf(CAPTURED_ROWS);
    }

    private static JobGraph sessionJob(Path fixture) {
        JobGraph.Builder job = JobGraph.named("out-of-order-session-acceptance");
        DataStream<ClickEvent> clicks = job.source("clicks", FileReplaySource.of(fixture, ClickEvent.class))
                .withEventTime(ClickEvent::eventTimeMillis, OUT_OF_ORDERNESS)
                .parallelism(1);

        clicks.keyBy("by-member", ClickEvent::memberId)
                .process("sessions", new SessionAggregator())
                .sink("capture", new CanonicalCapturingSink())
                .parallelism(1);
        return job.build();
    }

    private static Path fixturePath() throws URISyntaxException {
        return Path.of(SessionPipelineAcceptanceTest.class
                .getResource("out-of-order-clicks.jsonl")
                .toURI());
    }

    private static List<String> rows(List<byte[]> encodedRows) {
        List<String> rows = new ArrayList<>(encodedRows.size());
        for (byte[] encodedRow : encodedRows) {
            rows.add(new String(encodedRow, StandardCharsets.UTF_8));
        }
        return rows;
    }

    /** Captures a canonical byte representation without exposing runtime test hooks to the job. */
    private static final class CanonicalCapturingSink implements Operator<SessionRow, Void> {

        private static final long serialVersionUID = 1L;

        @Override
        public void processElement(StreamRecord<SessionRow> record, Collector<Void> out) {
            SessionRow row = record.value();
            String encoded = String.join("|",
                    row.memberId(),
                    Long.toString(row.sessionStartMillis()),
                    Long.toString(row.lastEventTimeMillis()),
                    Long.toString(row.sessionEndMillis()),
                    Long.toString(row.durationMillis()),
                    Long.toString(row.clickCount()),
                    String.join(",", row.searchTerms()));
            CAPTURED_ROWS.add(encoded.getBytes(StandardCharsets.UTF_8));
        }
    }
}
