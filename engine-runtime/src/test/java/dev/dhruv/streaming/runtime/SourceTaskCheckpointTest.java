package dev.dhruv.streaming.runtime;

import dev.dhruv.streaming.api.CheckpointBarrier;
import dev.dhruv.streaming.api.CheckpointableSource;
import dev.dhruv.streaming.api.Collector;
import dev.dhruv.streaming.api.SourceContext;
import dev.dhruv.streaming.api.Source;
import dev.dhruv.streaming.api.StreamElement;
import dev.dhruv.streaming.api.StreamRecord;
import dev.dhruv.streaming.api.TimestampAssigner;
import dev.dhruv.streaming.api.metrics.Counter;
import dev.dhruv.streaming.api.state.StateHandle;
import dev.dhruv.streaming.runtime.metrics.TaskMetricGroup;
import dev.dhruv.streaming.runtime.transport.Output;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Proves source snapshots and barriers are performed by the source thread, in stream order. */
class SourceTaskCheckpointTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void savesSourceAndWatermarkProgressBeforeForwardingTheBarrier() throws Exception {
        BlockingCheckpointableSource source = new BlockingCheckpointableSource();
        RecordingOutput output = new RecordingOutput();
        TaskMetricGroup metrics = new TaskMetricGroup("source", 0);
        SourceTask task = new SourceTask("source#0", source,
                new RuntimeSourceContext(0, 1, metrics), output,
                Optional.of((TimestampAssigner<String>) value -> 1_000L), 50L, 0L, metrics);
        CountDownLatch acknowledged = new CountDownLatch(1);
        AtomicReference<SourceTask.CheckpointResult> result = new AtomicReference<>();
        task.onCheckpoint(checkpoint -> {
            result.set(checkpoint);
            acknowledged.countDown();
        });

        Thread taskThread = new Thread(task, "source-checkpoint-test");
        taskThread.start();
        assertThat(source.firstRecordEmitted.await(2, TimeUnit.SECONDS)).isTrue();

        task.triggerCheckpoint(new CheckpointBarrier(7, 123L), temporaryDirectory.resolve("checkpoint"));
        source.allowNextPoll.countDown();
        assertThat(acknowledged.await(2, TimeUnit.SECONDS)).isTrue();

        task.cancel();
        taskThread.join(2_000);

        assertThat(source.snapshotThread.get()).isEqualTo(taskThread);
        assertThat(output.elements).containsSubsequence(
                new StreamRecord<>("record", 1_000L), new CheckpointBarrier(7, 123L));
        assertThat(result.get().stateHandle()).isPresent();
        String snapshot = Files.readString(Path.of(result.get().stateHandle().orElseThrow().uri()));
        assertThat(snapshot).contains("watermark.max-event-time=1000");
        assertThat(snapshot).contains("source.handle.path=source-state.txt");
    }

    @Test
    void connectorWakeupDuringCancellationIsNotReportedAsATaskFailure() throws Exception {
        CountDownLatch polling = new CountDownLatch(1);
        Source<String> source = new Source<>() {
            @Override
            public void open(SourceContext context) {
            }

            @Override
            public boolean poll(Collector<String> out) {
                polling.countDown();
                try {
                    Thread.sleep(10_000);
                    return true;
                } catch (InterruptedException interrupted) {
                    throw new IllegalStateException("connector wakeup", interrupted);
                }
            }

            @Override
            public void close() {
            }
        };
        TaskMetricGroup metrics = new TaskMetricGroup("source", 0);
        SourceTask task = new SourceTask("source#0", source,
                new RuntimeSourceContext(0, 1, metrics), new RecordingOutput(), Optional.empty(),
                metrics);
        AtomicBoolean failureReported = new AtomicBoolean();
        task.onFailure((ignored, failure) -> failureReported.set(true));

        Thread thread = new Thread(task, "cancel-source-test");
        thread.start();
        assertThat(polling.await(2, TimeUnit.SECONDS)).isTrue();
        task.cancel();
        thread.interrupt();
        thread.join(2_000);

        assertThat(thread.isAlive()).isFalse();
        assertThat(failureReported).isFalse();
    }

    private static final class BlockingCheckpointableSource implements CheckpointableSource<String> {
        private final AtomicBoolean emitted = new AtomicBoolean();
        private final CountDownLatch firstRecordEmitted = new CountDownLatch(1);
        private final CountDownLatch allowNextPoll = new CountDownLatch(1);
        private final AtomicReference<Thread> snapshotThread = new AtomicReference<>();

        @Override
        public void open(SourceContext context) {
        }

        @Override
        public boolean poll(Collector<String> out) throws Exception {
            if (emitted.compareAndSet(false, true)) {
                out.collect("record");
                firstRecordEmitted.countDown();
            }
            allowNextPoll.await(2, TimeUnit.SECONDS);
            return true;
        }

        @Override
        public StateHandle snapshot(long checkpointId, Path checkpointDir) throws java.io.IOException {
            snapshotThread.set(Thread.currentThread());
            Files.createDirectories(checkpointDir);
            Path state = checkpointDir.resolve("source-state.txt");
            Files.writeString(state, "next=1");
            return new StateHandle(state.toUri(), Files.size(state));
        }

        @Override
        public void restore(StateHandle handle) {
        }

        @Override
        public void close() {
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
