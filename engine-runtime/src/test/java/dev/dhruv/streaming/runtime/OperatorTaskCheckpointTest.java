package dev.dhruv.streaming.runtime;

import dev.dhruv.streaming.api.CheckpointBarrier;
import dev.dhruv.streaming.api.Collector;
import dev.dhruv.streaming.api.Either;
import dev.dhruv.streaming.api.IntervalJoinOperator;
import dev.dhruv.streaming.api.KeySelector;
import dev.dhruv.streaming.api.KeyedOperator;
import dev.dhruv.streaming.api.Operator;
import dev.dhruv.streaming.api.OperatorContext;
import dev.dhruv.streaming.api.StreamElement;
import dev.dhruv.streaming.api.StreamRecord;
import dev.dhruv.streaming.api.Watermark;
import dev.dhruv.streaming.api.state.ValueState;
import dev.dhruv.streaming.runtime.metrics.TaskMetricGroup;
import dev.dhruv.streaming.runtime.transport.InputGate;
import dev.dhruv.streaming.runtime.transport.Output;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end proofs for the barrier branch in the task run loop. */
class OperatorTaskCheckpointTest {

    private static final List<String> TIMER_RESULTS = new CopyOnWriteArrayList<>();

    @TempDir
    Path temporaryDirectory;

    @AfterEach
    void clearResults() {
        TIMER_RESULTS.clear();
    }

    @Test
    void forwardsAndAcknowledgesBeforeReleasingPostBarrierRecords() throws Exception {
        InputGate gate = new InputGate(2);
        RecordingOutput output = new RecordingOutput();
        List<String> processed = new CopyOnWriteArrayList<>();
        Operator<String, String> operator = (record, out) -> processed.add(record.value());
        OperatorTask task = new OperatorTask("operator#0", operator, gate, output,
                Optional.empty(), new InMemoryStateBackend(), temporaryDirectory,
                new TaskMetricGroup("operator", 0));

        CountDownLatch acknowledged = new CountDownLatch(1);
        AtomicBoolean barrierWasForwardedBeforeAck = new AtomicBoolean();
        AtomicBoolean bothChannelsStillBlockedAtAck = new AtomicBoolean();
        task.onCheckpoint(result -> {
            barrierWasForwardedBeforeAck.set(output.elements.contains(
                    new CheckpointBarrier(9, 123L)));
            bothChannelsStillBlockedAtAck.set(gate.blockedChannelCount() == 2);
            acknowledged.countDown();
        });

        Thread thread = new Thread(task, "operator-checkpoint-order");
        thread.start();
        gate.enqueue(0, new CheckpointBarrier(9, 123L));
        gate.enqueue(0, new StreamRecord<>("post-barrier", 2));
        gate.enqueue(1, new StreamRecord<>("pre-barrier", 1));
        gate.enqueue(1, new CheckpointBarrier(9, 123L));

        assertThat(acknowledged.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(barrierWasForwardedBeforeAck).isTrue();
        assertThat(bothChannelsStillBlockedAtAck).isTrue();
        awaitContains(processed, "post-barrier");
        assertThat(processed).containsExactly("pre-barrier", "post-barrier");
        assertThat(gate.blockedChannelCount()).isZero();

        gate.enqueue(0, Watermark.MAX);
        gate.enqueue(1, Watermark.MAX);
        thread.join(2_000);
        assertThat(thread.isAlive()).isFalse();
    }

    @Test
    void restoresKeyedStateAndPendingTimersFromOneTaskEnvelope() throws Exception {
        KeySelector<Event, String> selector = Event::key;
        InputGate firstGate = new InputGate(1);
        OperatorTask first = new OperatorTask("sessions#0", new TimerOperator(), firstGate,
                new RecordingOutput(), Optional.of(selector), new InMemoryStateBackend(),
                temporaryDirectory.resolve("first"), new TaskMetricGroup("sessions", 0));
        CountDownLatch snapshotted = new CountDownLatch(1);
        java.util.concurrent.atomic.AtomicReference<OperatorTask.CheckpointResult> checkpoint =
                new java.util.concurrent.atomic.AtomicReference<>();
        first.onCheckpoint(result -> {
            checkpoint.set(result);
            snapshotted.countDown();
        });

        Thread firstThread = new Thread(first, "before-recovery");
        firstThread.start();
        firstGate.enqueue(0, new StreamRecord<>(new Event("member-1", "remembered"), 10));
        firstGate.enqueue(0, new CheckpointBarrier(3, 11));
        assertThat(snapshotted.await(2, TimeUnit.SECONDS)).isTrue();
        first.cancel();
        firstThread.join(2_000);

        InputGate restoredGate = new InputGate(1);
        OperatorTask restored = new OperatorTask("sessions#0", new TimerOperator(), restoredGate,
                new RecordingOutput(), Optional.of(selector), new InMemoryStateBackend(),
                temporaryDirectory.resolve("restored"), new TaskMetricGroup("sessions", 0));
        restored.restore(checkpoint.get().stateHandle());
        Thread restoredThread = new Thread(restored, "after-recovery");
        restoredThread.start();
        restoredGate.enqueue(0, new Watermark(100));
        restoredGate.enqueue(0, Watermark.MAX);
        restoredThread.join(2_000);

        assertThat(restoredThread.isAlive()).isFalse();
        assertThat(TIMER_RESULTS).containsExactly("member-1:remembered");
    }

    @Test
    void restoresIntervalJoinBuffersAndTheirWatermarkCleanupTimers() throws Exception {
        KeySelector<Either<String, String>, String> selector = ignored -> "member-item";
        InputGate firstGate = new InputGate(1);
        OperatorTask first = new OperatorTask("join#0", stringJoin(), firstGate,
                new RecordingOutput(), Optional.of(selector), new InMemoryStateBackend(),
                temporaryDirectory.resolve("join-first"), new TaskMetricGroup("join", 0));
        CountDownLatch snapshotted = new CountDownLatch(1);
        java.util.concurrent.atomic.AtomicReference<OperatorTask.CheckpointResult> checkpoint =
                new java.util.concurrent.atomic.AtomicReference<>();
        first.onCheckpoint(result -> {
            checkpoint.set(result);
            snapshotted.countDown();
        });

        Thread firstThread = new Thread(first, "join-before-recovery");
        firstThread.start();
        firstGate.enqueue(0, new StreamRecord<>(Either.left("click"), 100));
        firstGate.enqueue(0, new CheckpointBarrier(7, 101));
        assertThat(snapshotted.await(2, TimeUnit.SECONDS)).isTrue();
        first.cancel();
        firstThread.join(2_000);

        InputGate restoredGate = new InputGate(1);
        RecordingOutput restoredOutput = new RecordingOutput();
        TaskMetricGroup restoredMetrics = new TaskMetricGroup("join", 0);
        OperatorTask restored = new OperatorTask("join#0", stringJoin(), restoredGate,
                restoredOutput, Optional.of(selector), new InMemoryStateBackend(),
                temporaryDirectory.resolve("join-restored"), restoredMetrics);
        restored.restore(checkpoint.get().stateHandle());
        Thread restoredThread = new Thread(restored, "join-after-recovery");
        restoredThread.start();

        restoredGate.enqueue(0, new StreamRecord<>(Either.right("borrow"), 130));
        awaitRecord(restoredOutput.elements, "click/borrow");
        assertThat(restoredMetrics.snapshot())
                .containsEntry("left-buffer-size", 1L)
                .containsEntry("right-buffer-size", 1L);

        // The left cleanup timer at 131 was registered before the checkpoint. The restored
        // right record only registers a timer at 161, so draining the left side here proves the
        // earlier timer was included in the task envelope and restored with its keyed state.
        restoredGate.enqueue(0, new Watermark(131));
        awaitMetric(restoredMetrics, "left-buffer-size", 0);
        assertThat(restoredMetrics.snapshot()).containsEntry("right-buffer-size", 1L);

        restoredGate.enqueue(0, Watermark.MAX);
        restoredThread.join(2_000);
        assertThat(restoredThread.isAlive()).isFalse();
    }

    @Test
    void coordinatorAbortReleasesAChannelAndAllowsTheNextCheckpoint() throws Exception {
        InputGate gate = new InputGate(2);
        RecordingOutput output = new RecordingOutput();
        OperatorTask task = new OperatorTask("operator#0", (record, out) -> { }, gate, output,
                Optional.empty(), new InMemoryStateBackend(), temporaryDirectory,
                new TaskMetricGroup("operator", 0));
        CountDownLatch secondCheckpoint = new CountDownLatch(1);
        task.onCheckpoint(result -> {
            if (result.checkpointId() == 2) {
                secondCheckpoint.countDown();
            }
        });

        Thread thread = new Thread(task, "operator-abort-alignment");
        thread.start();
        gate.enqueue(0, new CheckpointBarrier(1, 1));
        awaitBlockedChannels(gate, 1);

        task.abortCheckpoint(1);
        assertThat(gate.blockedChannelCount()).isZero();
        gate.enqueue(0, new CheckpointBarrier(2, 2));
        gate.enqueue(1, new CheckpointBarrier(2, 2));

        assertThat(secondCheckpoint.await(2, TimeUnit.SECONDS)).isTrue();
        gate.enqueue(0, Watermark.MAX);
        gate.enqueue(1, Watermark.MAX);
        thread.join(2_000);
        assertThat(thread.isAlive()).isFalse();
    }

    @Test
    void timeoutWhileAcknowledgementIsBlockedDoesNotFailTheTask() throws Exception {
        InputGate gate = new InputGate(2);
        OperatorTask task = new OperatorTask("operator#0", (record, out) -> { }, gate,
                new RecordingOutput(), Optional.empty(), new InMemoryStateBackend(),
                temporaryDirectory, new TaskMetricGroup("operator", 0));
        CountDownLatch listenerEntered = new CountDownLatch(1);
        CountDownLatch releaseListener = new CountDownLatch(1);
        CountDownLatch secondCheckpoint = new CountDownLatch(1);
        AtomicBoolean failed = new AtomicBoolean();
        task.onFailure((ignored, failure) -> failed.set(true));
        task.onCheckpoint(result -> {
            if (result.checkpointId() == 1) {
                listenerEntered.countDown();
                try {
                    releaseListener.await(2, TimeUnit.SECONDS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            } else if (result.checkpointId() == 2) {
                secondCheckpoint.countDown();
            }
        });

        Thread thread = new Thread(task, "operator-abort-during-ack");
        thread.start();
        gate.enqueue(0, new CheckpointBarrier(1, 1));
        gate.enqueue(1, new CheckpointBarrier(1, 1));
        assertThat(listenerEntered.await(2, TimeUnit.SECONDS)).isTrue();

        task.abortCheckpoint(1);
        releaseListener.countDown();
        gate.enqueue(0, new CheckpointBarrier(2, 2));
        gate.enqueue(1, new CheckpointBarrier(2, 2));

        assertThat(secondCheckpoint.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(failed).isFalse();
        gate.enqueue(0, Watermark.MAX);
        gate.enqueue(1, Watermark.MAX);
        thread.join(2_000);
        assertThat(thread.isAlive()).isFalse();
    }

    private static void awaitContains(List<String> values, String expected)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (!values.contains(expected) && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
    }

    private static void awaitRecord(List<StreamElement> elements, Object expected)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() < deadline) {
            if (elements.stream().filter(StreamRecord.class::isInstance)
                    .map(StreamRecord.class::cast)
                    .anyMatch(record -> expected.equals(record.value()))) {
                return;
            }
            Thread.sleep(10);
        }
        assertThat(elements).anyMatch(element -> element instanceof StreamRecord<?> record
                && expected.equals(record.value()));
    }

    private static void awaitMetric(TaskMetricGroup metrics, String name, long expected)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (metrics.snapshot().getOrDefault(name, Long.MIN_VALUE) != expected
                && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(metrics.snapshot()).containsEntry(name, expected);
    }

    private static IntervalJoinOperator<String, String, String, String> stringJoin() {
        return new IntervalJoinOperator<>(0, 30, (left, right) -> left + "/" + right);
    }

    private static void awaitBlockedChannels(InputGate gate, int expected)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (gate.blockedChannelCount() != expected && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(gate.blockedChannelCount()).isEqualTo(expected);
    }

    private record Event(String key, String value) implements java.io.Serializable {
    }

    private static final class TimerOperator implements KeyedOperator<String, Event, String> {
        private transient ValueState<String> state;
        private transient OperatorContext context;

        @Override
        public void open(OperatorContext context) {
            this.context = context;
            state = context.getValueState("value", String.class);
        }

        @Override
        public void processElement(StreamRecord<Event> record, Collector<String> out)
                throws Exception {
            state.update(record.value().value());
            context.registerEventTimer(100);
        }

        @Override
        public void onEventTimer(long timestamp, String key, Collector<String> out)
                throws Exception {
            TIMER_RESULTS.add(key + ":" + state.value().orElseThrow());
        }
    }

    private static final class RecordingOutput implements Output {
        private final List<StreamElement> elements = new CopyOnWriteArrayList<>();

        @Override
        public void emit(StreamRecord<?> record) {
            elements.add(record);
        }

        @Override
        public void broadcast(StreamElement element) {
            elements.add(element);
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    }
}
