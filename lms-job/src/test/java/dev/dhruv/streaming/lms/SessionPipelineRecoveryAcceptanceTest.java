package dev.dhruv.streaming.lms;

import dev.dhruv.streaming.api.CheckpointBarrier;
import dev.dhruv.streaming.api.Collector;
import dev.dhruv.streaming.api.JobExecutor;
import dev.dhruv.streaming.api.JobExecutors;
import dev.dhruv.streaming.api.KeySelector;
import dev.dhruv.streaming.api.Operator;
import dev.dhruv.streaming.api.StreamElement;
import dev.dhruv.streaming.api.StreamRecord;
import dev.dhruv.streaming.api.TimestampAssigner;
import dev.dhruv.streaming.api.Watermark;
import dev.dhruv.streaming.api.graph.DataStream;
import dev.dhruv.streaming.api.graph.JobGraph;
import dev.dhruv.streaming.api.state.StateHandle;
import dev.dhruv.streaming.connectors.file.FileReplaySource;
import dev.dhruv.streaming.runtime.OperatorTask;
import dev.dhruv.streaming.runtime.RocksDbStateBackend;
import dev.dhruv.streaming.runtime.RuntimeSourceContext;
import dev.dhruv.streaming.runtime.SourceTask;
import dev.dhruv.streaming.runtime.metrics.TaskMetricGroup;
import dev.dhruv.streaming.runtime.transport.InputGate;
import dev.dhruv.streaming.runtime.transport.Output;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Phase 4 recovery proof, deliberately a pipeline test rather than three isolated snapshot
 * tests. The checkpoint sits after two alpha events but before their session timer fires. The
 * restored source has only beta left to replay; beta's watermark must fire alpha's restored
 * timer against alpha's restored keyed state. Replaying the source prefix, dropping the state,
 * or dropping the timer each produces different canonical bytes.
 */
class SessionPipelineRecoveryAcceptanceTest {

    private static final Duration OUT_OF_ORDERNESS = Duration.ofSeconds(5);

    @TempDir
    Path temporaryDirectory;

    @Test
    @Timeout(15)
    void wholeJobRecoveryMidWindowMatchesACleanReplayByteForByte() throws Exception {
        Path fixture = writeFixture();

        List<byte[]> clean = runClean(fixture);
        Checkpoint checkpoint = stopWholeJobAfterMidWindowCheckpoint(fixture);
        List<byte[]> recovered = restoreWholeJobAndFinish(fixture, checkpoint);

        assertThat(clean).hasSize(2);
        assertBytesEqual(recovered, clean);
        assertThat(asText(recovered)).containsExactly(
                "member-alpha|18000|20000|920000|2000|2|first,second",
                "member-beta|2000000|2000000|2900000|0|1|final");
    }

    private Checkpoint stopWholeJobAfterMidWindowCheckpoint(Path fixture) throws Exception {
        Path checkpoints = temporaryDirectory.resolve("checkpoint-storage");
        InputGate gate = new InputGate(1);
        InputGate sinkGate = new InputGate(1);
        CountDownLatch sourceAcknowledged = new CountDownLatch(1);
        CountDownLatch sessionAcknowledged = new CountDownLatch(1);
        CountDownLatch sinkAcknowledged = new CountDownLatch(1);
        CountDownLatch allowSourceToStop = new CountDownLatch(1);
        AtomicReference<StateHandle> sourceHandle = new AtomicReference<>();
        AtomicReference<StateHandle> sessionHandle = new AtomicReference<>();
        AtomicReference<StateHandle> sinkHandle = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CheckpointBarrier barrier = new CheckpointBarrier(1, 0);

        CheckpointInjectingOutput sourceOutput = new CheckpointInjectingOutput(gate, barrier,
                checkpoints.resolve("source"));
        SourceTask source = new SourceTask("clicks:0", FileReplaySource.of(fixture, ClickEvent.class),
                new RuntimeSourceContext(0, 1, new TaskMetricGroup("clicks", 0)), sourceOutput,
                Optional.of((TimestampAssigner<ClickEvent>) ClickEvent::eventTimeMillis),
                OUT_OF_ORDERNESS.toMillis(), 0,
                new TaskMetricGroup("clicks", 0));
        sourceOutput.bind(source);
        source.onFailure((ignored, throwable) -> failure.compareAndSet(null, throwable));
        source.onCheckpoint(result -> {
            sourceHandle.set(result.stateHandle().orElseThrow());
            sourceAcknowledged.countDown();
            await(allowSourceToStop);
        });

        OperatorTask sessions = sessionTask("sessions:0", gate, new GateOutput(sinkGate),
                temporaryDirectory.resolve("first-live-db"), checkpoints.resolve("sessions"));
        sessions.onFailure((ignored, throwable) -> failure.compareAndSet(null, throwable));
        sessions.onCheckpoint(result -> {
            sessionHandle.set(result.stateHandle());
            sessionAcknowledged.countDown();
        });
        CollectingSink firstSink = new CollectingSink();
        OperatorTask sink = sinkTask("capture:0", sinkGate, firstSink,
                temporaryDirectory.resolve("first-sink-live-db"), checkpoints.resolve("capture"));
        sink.onFailure((ignored, throwable) -> failure.compareAndSet(null, throwable));
        sink.onCheckpoint(result -> {
            sinkHandle.set(result.stateHandle());
            sinkAcknowledged.countDown();
        });

        Thread sinkThread = new Thread(sink, "capture-before-loss");
        Thread operatorThread = new Thread(sessions, "sessions-before-loss");
        Thread sourceThread = new Thread(source, "source-before-loss");
        sinkThread.start();
        operatorThread.start();
        sourceThread.start();

        assertThat(sourceAcknowledged.await(5, TimeUnit.SECONDS))
                .as("the source cursor must be snapshotted before the injected barrier")
                .isTrue();
        assertThat(sessionAcknowledged.await(5, TimeUnit.SECONDS))
                .as("the open session and its pending event-time timer must acknowledge")
                .isTrue();
        assertThat(sinkAcknowledged.await(5, TimeUnit.SECONDS))
                .as("the downstream sink task must also participate in the consistent cut")
                .isTrue();

        // This is the task-loss cut: no task gets to drain or emit the remaining source suffix.
        // Recovery intentionally redeploys both tasks, rather than attempting to revive just the
        // operator, because one task alone could combine incompatible stream prefixes.
        source.cancel();
        sessions.cancel();
        sink.cancel();
        allowSourceToStop.countDown();
        join(sourceThread);
        join(operatorThread);
        join(sinkThread);
        assertThat(failure.get()).isNull();

        return new Checkpoint(sourceHandle.get(), sessionHandle.get(), sinkHandle.get());
    }

    private List<byte[]> restoreWholeJobAndFinish(Path fixture, Checkpoint checkpoint) throws Exception {
        InputGate gate = new InputGate(1);
        InputGate sinkGate = new InputGate(1);
        SourceTask source = new SourceTask("clicks:0", FileReplaySource.of(fixture, ClickEvent.class),
                new RuntimeSourceContext(0, 1, new TaskMetricGroup("clicks", 0)),
                new GateOutput(gate), Optional.of((TimestampAssigner<ClickEvent>) ClickEvent::eventTimeMillis),
                OUT_OF_ORDERNESS.toMillis(), 0, new TaskMetricGroup("clicks", 0));
        source.restore(checkpoint.source());

        // A fresh directory is intentional. Passing the old live database would make the test
        // accidentally succeed without exercising the StateHandle restore path.
        OperatorTask sessions = sessionTask("sessions:0", gate, new GateOutput(sinkGate),
                temporaryDirectory.resolve("restored-live-db"),
                temporaryDirectory.resolve("checkpoint-storage/sessions"));
        sessions.restore(checkpoint.session());
        CollectingSink captured = new CollectingSink();
        OperatorTask sink = sinkTask("capture:0", sinkGate, captured,
                temporaryDirectory.resolve("restored-sink-live-db"),
                temporaryDirectory.resolve("checkpoint-storage/capture"));
        sink.restore(checkpoint.sink());
        AtomicReference<Throwable> failure = new AtomicReference<>();
        source.onFailure((ignored, throwable) -> failure.compareAndSet(null, throwable));
        sessions.onFailure((ignored, throwable) -> failure.compareAndSet(null, throwable));
        sink.onFailure((ignored, throwable) -> failure.compareAndSet(null, throwable));

        Thread sinkThread = new Thread(sink, "capture-after-recovery");
        Thread operatorThread = new Thread(sessions, "sessions-after-recovery");
        Thread sourceThread = new Thread(source, "source-after-recovery");
        sinkThread.start();
        operatorThread.start();
        sourceThread.start();
        join(sourceThread);
        join(operatorThread);
        join(sinkThread);

        assertThat(failure.get()).isNull();
        return captured.rows();
    }

    private OperatorTask sessionTask(String taskId, InputGate gate, Output output, Path liveState,
                                     Path checkpoints) throws IOException {
        return new OperatorTask(taskId, new SessionAggregator(), gate, output,
                Optional.of((KeySelector<ClickEvent, String>) ClickEvent::memberId),
                new RocksDbStateBackend(liveState), checkpoints, new TaskMetricGroup("sessions", 0));
    }

    private OperatorTask sinkTask(String taskId, InputGate gate, Operator<SessionRow, Void> operator,
                                  Path liveState, Path checkpoints) throws IOException {
        return new OperatorTask(taskId, operator, gate, new DiscardingOutput(), Optional.empty(),
                new RocksDbStateBackend(liveState), checkpoints, new TaskMetricGroup("capture", 0));
    }

    private List<byte[]> runClean(Path fixture) throws Exception {
        CleanSink.clear();
        try (JobExecutor executor = JobExecutors.local(cleanJob(fixture))) {
            executor.start();
            executor.awaitTermination();
        }
        return CleanSink.rows();
    }

    private static JobGraph cleanJob(Path fixture) {
        JobGraph.Builder job = JobGraph.named("phase-four-clean-session-replay");
        DataStream<ClickEvent> clicks = job.source("clicks", FileReplaySource.of(fixture, ClickEvent.class))
                .withEventTime(ClickEvent::eventTimeMillis, OUT_OF_ORDERNESS)
                .parallelism(1);
        clicks.keyBy("by-member", ClickEvent::memberId)
                .process("sessions", new SessionAggregator())
                .sink("capture", new CleanSink())
                .parallelism(1);
        return job.build();
    }

    private Path writeFixture() throws IOException {
        Path fixture = temporaryDirectory.resolve("mid-window-recovery.jsonl");
        Files.writeString(fixture, String.join("\n",
                "{\"memberId\":\"member-alpha\",\"catalogItemId\":\"book-1\",\"searchTerm\":\"first\",\"eventType\":\"SEARCH\",\"eventTimeMillis\":18000}",
                "{\"memberId\":\"member-alpha\",\"catalogItemId\":\"book-1\",\"searchTerm\":\"second\",\"eventType\":\"RESULT_CLICK\",\"eventTimeMillis\":20000}",
                "{\"memberId\":\"member-beta\",\"catalogItemId\":\"book-2\",\"searchTerm\":\"final\",\"eventType\":\"SEARCH\",\"eventTimeMillis\":2000000}") + "\n");
        return fixture;
    }

    private static void assertBytesEqual(List<byte[]> actual, List<byte[]> expected) {
        assertThat(actual).hasSameSizeAs(expected);
        for (int index = 0; index < expected.size(); index++) {
            assertThat(actual.get(index)).containsExactly(expected.get(index));
        }
    }

    private static List<String> asText(List<byte[]> rows) {
        return rows.stream().map(row -> new String(row, StandardCharsets.UTF_8)).toList();
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("timed out waiting to stop source at checkpoint");
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting to stop source", exception);
        }
    }

    private static void join(Thread thread) throws InterruptedException {
        thread.join(TimeUnit.SECONDS.toMillis(5));
        assertThat(thread.isAlive()).as("task thread " + thread.getName() + " must stop").isFalse();
    }

    private record Checkpoint(StateHandle source, StateHandle session, StateHandle sink) {
    }

    /** In-memory transport used by the acceptance harness; it preserves source order in one gate. */
    private static class GateOutput implements Output {
        private final InputGate gate;

        private GateOutput(InputGate gate) {
            this.gate = gate;
        }

        @Override
        public void emit(StreamRecord<?> record) throws InterruptedException {
            gate.enqueue(0, record);
        }

        @Override
        public void broadcast(StreamElement element) throws InterruptedException {
            gate.enqueue(0, element);
        }

        @Override
        public void flush() {
            // The handoff is immediate rather than buffered.
        }

        @Override
        public void close() {
            // Input completion is represented by Watermark.MAX, not transport closure.
        }
    }

    /** Injects exactly one checkpoint after the second physical source record. */
    private static final class CheckpointInjectingOutput extends GateOutput {
        private final CheckpointBarrier barrier;
        private final Path checkpointDirectory;
        private final AtomicReference<SourceTask> sourceTask = new AtomicReference<>();
        private final AtomicBoolean triggered = new AtomicBoolean();
        private int records;

        private CheckpointInjectingOutput(InputGate gate, CheckpointBarrier barrier,
                                          Path checkpointDirectory) {
            super(gate);
            this.barrier = barrier;
            this.checkpointDirectory = checkpointDirectory;
        }

        private void bind(SourceTask task) {
            sourceTask.set(task);
        }

        @Override
        public void emit(StreamRecord<?> record) throws InterruptedException {
            super.emit(record);
            records++;
            if (records == 2 && triggered.compareAndSet(false, true)) {
                sourceTask.get().triggerCheckpoint(barrier, checkpointDirectory);
            }
        }
    }

    /** The terminal transport after the capturing sink; it deliberately has no downstream task. */
    private static final class DiscardingOutput implements Output {
        @Override
        public void emit(StreamRecord<?> record) {
        }

        @Override
        public void broadcast(StreamElement element) {
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    }

    /** Captures rows inside the real downstream sink task, not at the session task boundary. */
    private static final class CollectingSink implements Operator<SessionRow, Void> {
        private final List<byte[]> rows = new ArrayList<>();

        @Override
        public void processElement(StreamRecord<SessionRow> record, Collector<Void> out) {
            SessionRow row = record.value();
            rows.add(encode(row));
        }

        private List<byte[]> rows() {
            return List.copyOf(rows);
        }
    }

    /** The clean public-API baseline uses the same canonical row encoding as the recovery run. */
    private static final class CleanSink implements Operator<SessionRow, Void> {
        private static final List<byte[]> ROWS = new ArrayList<>();

        private static void clear() {
            ROWS.clear();
        }

        private static List<byte[]> rows() {
            return List.copyOf(ROWS);
        }

        @Override
        public void processElement(StreamRecord<SessionRow> record, Collector<Void> out) {
            ROWS.add(encode(record.value()));
        }
    }

    private static byte[] encode(SessionRow row) {
        return String.join("|", row.memberId(), Long.toString(row.sessionStartMillis()),
                Long.toString(row.lastEventTimeMillis()), Long.toString(row.sessionEndMillis()),
                Long.toString(row.durationMillis()), Long.toString(row.clickCount()),
                String.join(",", row.searchTerms())).getBytes(StandardCharsets.UTF_8);
    }
}
