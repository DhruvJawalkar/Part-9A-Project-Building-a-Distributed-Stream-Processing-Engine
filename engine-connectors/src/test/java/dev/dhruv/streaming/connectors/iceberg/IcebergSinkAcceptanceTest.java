package dev.dhruv.streaming.connectors.iceberg;

import dev.dhruv.streaming.api.CheckpointBarrier;
import dev.dhruv.streaming.api.Collector;
import dev.dhruv.streaming.api.OperatorContext;
import dev.dhruv.streaming.api.StreamElement;
import dev.dhruv.streaming.api.StreamRecord;
import dev.dhruv.streaming.api.Watermark;
import dev.dhruv.streaming.api.metrics.Counter;
import dev.dhruv.streaming.api.metrics.Histogram;
import dev.dhruv.streaming.api.metrics.MetricGroup;
import dev.dhruv.streaming.runtime.InMemoryStateBackend;
import dev.dhruv.streaming.runtime.OperatorTask;
import dev.dhruv.streaming.runtime.metrics.TaskMetricGroup;
import dev.dhruv.streaming.runtime.transport.InputGate;
import dev.dhruv.streaming.runtime.transport.Output;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 6 acceptance evidence for the Iceberg checkpoint transaction boundary.
 *
 * <p>The cadence figures use a logical input clock rather than wall time. That makes the
 * visibility-delay comparison stable enough to cite in the README: its numbers describe the
 * checkpoint policy, not CI-machine scheduling.
 */
class IcebergSinkAcceptanceTest {

    private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "value",
            Types.StringType.get()));
    private static final List<Event> SIX_EVENTS = List.of(
            new Event("event-0", 0), new Event("event-1", 100), new Event("event-2", 200),
            new Event("event-3", 300), new Event("event-4", 400), new Event("event-5", 500));

    @TempDir
    Path temporaryDirectory;

    @Test
    void keepsRowsAndFilesInvisibleUntilOneCheckpointCompletion() throws Exception {
        TableFixture fixture = table("invisible-until-complete");
        IcebergSink<Event> sink = sink(fixture);
        sink.open(new TestOperatorContext());
        sink.processElement(record("one", 10), discardingCollector());
        sink.processElement(record("two", 20), discardingCollector());
        sink.processElement(record("three", 30), discardingCollector());

        // This is the middle of an interval: Parquet is being written, but Iceberg sees nothing.
        assertThat(scan(fixture.table())).isEqualTo(new TableEvidence(0, 0, List.of(), List.of()));

        IcebergSink.IcebergSinkState prepared =
                (IcebergSink.IcebergSinkState) sink.preCommit(1);
        String closedPath = prepared.pendingFiles().get(1L).getFirst().path();
        assertThat(fixture.table().io().newInputFile(closedPath).exists()).isTrue();
        assertThat(scan(fixture.table())).isEqualTo(new TableEvidence(0, 0, List.of(), List.of()));

        sink.notifyCheckpointComplete(1);
        assertThat(scan(fixture.table())).isEqualTo(new TableEvidence(1, 1,
                List.of(closedPath), List.of("one", "three", "two")));

        // Replayed coordinator completion is idempotent: no second data file or snapshot.
        sink.notifyCheckpointComplete(1);
        assertThat(scan(fixture.table())).isEqualTo(new TableEvidence(1, 1,
                List.of(closedPath), List.of("one", "three", "two")));
        sink.close();
    }

    @Test
    void workerLossBeforeCompletionOrphansClosedFileAndReplayPublishesEachRowOnce()
            throws Exception {
        TableFixture fixture = table("replay-after-worker-loss");
        List<Event> interval = List.of(new Event("member-7", 10), new Event("member-9", 20));

        IcebergSink<Event> lostWorker = sink(fixture);
        lostWorker.open(new TestOperatorContext());
        write(lostWorker, interval);
        IcebergSink.IcebergSinkState abandonedCheckpoint =
                (IcebergSink.IcebergSinkState) lostWorker.preCommit(7);
        String orphanPath = abandonedCheckpoint.pendingFiles().get(7L).getFirst().path();
        lostWorker.close(); // The worker dies before the coordinator declares checkpoint 7 complete.

        assertThat(fixture.table().io().newInputFile(orphanPath).exists()).isTrue();
        assertThat(scan(fixture.table())).isEqualTo(new TableEvidence(0, 0, List.of(), List.of()));

        // Checkpoint 7 was never complete, so source recovery replays its logical records.
        IcebergSink<Event> recoveredWorker = sink(fixture);
        recoveredWorker.open(new TestOperatorContext());
        write(recoveredWorker, interval);
        recoveredWorker.preCommit(8);
        recoveredWorker.notifyCheckpointComplete(8);

        TableEvidence recovered = scan(fixture.table());
        assertThat(recovered.snapshotCount()).isEqualTo(1);
        assertThat(recovered.dataFileCount()).isEqualTo(1);
        assertThat(recovered.rows()).containsExactly("member-7", "member-9");
        assertThat(recovered.dataPaths()).doesNotContain(orphanPath);
        assertThat(fixture.table().io().newInputFile(orphanPath).exists()).isTrue();
        recoveredWorker.close();
    }

    @Test
    void operatorTaskDeliversCompletionOnItsRunLoopBeforeMakingRowsVisible() throws Exception {
        TableFixture fixture = table("operator-task-lifecycle");
        InputGate gate = new InputGate(1);
        IcebergSink<Event> sink = sink(fixture);
        OperatorTask task = new OperatorTask("iceberg-sink#0", sink, gate, new NoopOutput(),
                java.util.Optional.empty(), new InMemoryStateBackend(),
                temporaryDirectory.resolve("operator-task"), new TaskMetricGroup("iceberg", 0));
        CountDownLatch prepared = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        task.onCheckpoint(ignored -> prepared.countDown());
        task.onFailure((ignored, cause) -> failure.set(cause));

        Thread worker = new Thread(task, "iceberg-acceptance-task");
        worker.start();
        gate.enqueue(0, record("runtime-one", 10));
        gate.enqueue(0, record("runtime-two", 20));
        gate.enqueue(0, new CheckpointBarrier(3, 20));
        assertThat(prepared.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(scan(fixture.table())).isEqualTo(new TableEvidence(0, 0, List.of(), List.of()));

        task.notifyCheckpointComplete(3).get(2, TimeUnit.SECONDS);
        TableEvidence completed = scan(fixture.table());
        assertThat(completed.snapshotCount()).isEqualTo(1);
        assertThat(completed.dataFileCount()).isEqualTo(1);
        assertThat(completed.rows()).containsExactly("runtime-one", "runtime-two");
        assertThat(failure.get()).isNull();

        gate.enqueue(0, Watermark.MAX);
        worker.join(Duration.ofSeconds(2).toMillis());
        assertThat(worker.isAlive()).isFalse();
    }

    @Test
    void shortCheckpointCadenceMakesRowsVisibleSoonerAndCreatesMoreFiles() throws Exception {
        CadenceEvidence shortCadence = runCadence("short-cadence", 2);
        CadenceEvidence longCadence = runCadence("long-cadence", 6);

        // Stable, derived figures for the Phase 6 README: 100 ms vs 500 ms to first visibility,
        // in exchange for three Parquet files/snapshots rather than one.
        assertThat(shortCadence).isEqualTo(new CadenceEvidence(100, 3, 3, 6));
        assertThat(longCadence).isEqualTo(new CadenceEvidence(500, 1, 1, 6));
        assertThat(shortCadence.firstVisibilityDelayMillis())
                .isLessThan(longCadence.firstVisibilityDelayMillis());
        assertThat(shortCadence.dataFileCount()).isGreaterThan(longCadence.dataFileCount());
    }

    private CadenceEvidence runCadence(String tableName, int recordsPerCheckpoint)
            throws Exception {
        TableFixture fixture = table(tableName);
        IcebergSink<Event> sink = sink(fixture);
        sink.open(new TestOperatorContext());
        long checkpointId = 0;
        long firstVisibleAt = -1;
        for (int index = 0; index < SIX_EVENTS.size(); index++) {
            Event event = SIX_EVENTS.get(index);
            sink.processElement(record(event.value(), event.arrivalMillis()), discardingCollector());
            if ((index + 1) % recordsPerCheckpoint == 0 || index == SIX_EVENTS.size() - 1) {
                checkpointId++;
                sink.preCommit(checkpointId);
                sink.notifyCheckpointComplete(checkpointId);
                if (firstVisibleAt < 0) {
                    firstVisibleAt = event.arrivalMillis();
                }
            }
        }
        TableEvidence evidence = scan(fixture.table());
        sink.close();
        return new CadenceEvidence(firstVisibleAt - SIX_EVENTS.getFirst().arrivalMillis(),
                evidence.dataFileCount(), evidence.snapshotCount(), evidence.rows().size());
    }

    private static void write(IcebergSink<Event> sink, List<Event> events) throws Exception {
        for (Event event : events) {
            sink.processElement(record(event.value(), event.arrivalMillis()), discardingCollector());
        }
    }

    private static StreamRecord<Event> record(String value, long timestamp) {
        return new StreamRecord<>(new Event(value, timestamp), timestamp);
    }

    private static IcebergSink<Event> sink(TableFixture fixture) {
        return new IcebergSink<>(fixture.loader(), event -> Map.of("value", event.value()),
                "phase-6-acceptance");
    }

    private static TableFixture table(String name) {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("phase-6", Map.of());
        Namespace namespace = Namespace.of("test");
        catalog.createNamespace(namespace);
        TableIdentifier identifier = TableIdentifier.of(namespace, name);
        Table table = catalog.createTable(identifier, SCHEMA, PartitionSpec.unpartitioned());
        return new TableFixture(table, () -> catalog.loadTable(identifier));
    }

    private static TableEvidence scan(Table table) throws Exception {
        table.refresh();
        List<String> dataPaths = new ArrayList<>();
        List<String> rows = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                dataPaths.add(task.file().location());
                try (CloseableIterable<Record> records = Parquet.read(table.io()
                        .newInputFile(task.file().location()))
                        .project(table.schema())
                        .createReaderFunc(fileSchema ->
                                GenericParquetReaders.buildReader(table.schema(), fileSchema))
                        .build()) {
                    for (Record row : records) {
                        rows.add((String) row.getField("value"));
                    }
                }
            }
        }
        dataPaths.sort(String::compareTo);
        rows.sort(String::compareTo);
        return new TableEvidence(snapshotCount(table), dataPaths.size(), List.copyOf(dataPaths),
                List.copyOf(rows));
    }

    private static int snapshotCount(Table table) {
        int count = 0;
        for (Object ignored : table.snapshots()) {
            count++;
        }
        return count;
    }

    private static Collector<Void> discardingCollector() {
        return new Collector<>() {
            @Override
            public void collect(Void value) {
            }

            @Override
            public void collect(Void value, long timestamp) {
            }
        };
    }

    private record Event(String value, long arrivalMillis) {
    }

    private record TableFixture(Table table, TableLoader loader) {
    }

    private record TableEvidence(int snapshotCount, int dataFileCount, List<String> dataPaths,
                                 List<String> rows) {
    }

    private record CadenceEvidence(long firstVisibilityDelayMillis, int dataFileCount,
                                   int snapshotCount, int rowCount) {
    }

    private record TestOperatorContext() implements OperatorContext {
        @Override
        public <T> dev.dhruv.streaming.api.state.ValueState<T> getValueState(String name,
                Class<T> type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> dev.dhruv.streaming.api.state.ListState<T> getListState(String name,
                Class<T> type) {
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

    private static final class NoopOutput implements Output {
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
}
