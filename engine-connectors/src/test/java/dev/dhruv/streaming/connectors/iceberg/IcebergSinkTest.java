package dev.dhruv.streaming.connectors.iceberg;

import dev.dhruv.streaming.api.Collector;
import dev.dhruv.streaming.api.OperatorContext;
import dev.dhruv.streaming.api.StreamRecord;
import dev.dhruv.streaming.api.metrics.Counter;
import dev.dhruv.streaming.api.metrics.Histogram;
import dev.dhruv.streaming.api.metrics.MetricGroup;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IcebergSinkTest {

    private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "value",
            Types.StringType.get()));

    @Test
    void keepsIntervalFilesInvisibleThenCommitsThemOnceAfterRecovery() throws Exception {
        InMemoryCatalog catalog = catalog();
        TableIdentifier identifier = TableIdentifier.of("test", "visible");
        Table table = catalog.createTable(identifier, SCHEMA, PartitionSpec.unpartitioned());
        TableLoader loader = () -> catalog.loadTable(identifier);

        IcebergSink<String> beforeFailure = sink(loader);
        beforeFailure.open(new TestOperatorContext());
        beforeFailure.processElement(new StreamRecord<>("one", 10), discardingCollector());
        beforeFailure.processElement(new StreamRecord<>("two", 20), discardingCollector());

        // Writing a Parquet file alone cannot make a row visible to an Iceberg scan.
        assertThat(dataFilePaths(table)).isEmpty();

        IcebergSink.IcebergSinkState state =
                (IcebergSink.IcebergSinkState) beforeFailure.preCommit(7);
        state = roundTrip(state);
        String pendingPath = state.pendingFiles().get(7L).getFirst().path();
        assertThat(table.io().newInputFile(pendingPath).exists()).isTrue();
        assertThat(dataFilePaths(table)).isEmpty();
        beforeFailure.close();

        // A recovered task gets the prepared file path from its operator checkpoint, not from
        // the old writer instance. Completion appends it in one table metadata commit.
        IcebergSink<String> recovered = sink(loader);
        recovered.open(new TestOperatorContext());
        recovered.restoreCheckpointState(state);
        recovered.notifyCheckpointComplete(7);

        assertThat(dataFilePaths(table)).containsExactly(pendingPath);

        // The coordinator may replay completion after a crash between the metadata commit and
        // its acknowledgement. Scanning the current snapshot prevents a duplicate append.
        recovered.notifyCheckpointComplete(7);
        assertThat(dataFilePaths(table)).containsExactly(pendingPath);
        recovered.close();
    }

    @Test
    void leavesAbortedIntervalFilesUnreferenced() throws Exception {
        InMemoryCatalog catalog = catalog();
        TableIdentifier identifier = TableIdentifier.of("test", "aborted");
        Table table = catalog.createTable(identifier, SCHEMA, PartitionSpec.unpartitioned());
        IcebergSink<String> sink = sink(() -> catalog.loadTable(identifier));
        sink.open(new TestOperatorContext());
        sink.processElement(new StreamRecord<>("never-visible", 10), discardingCollector());

        IcebergSink.IcebergSinkState state = (IcebergSink.IcebergSinkState) sink.preCommit(11);
        String abandonedPath = state.pendingFiles().get(11L).getFirst().path();
        sink.notifyCheckpointAborted(11);

        assertThat(table.io().newInputFile(abandonedPath).exists()).isTrue();
        assertThat(dataFilePaths(table)).isEmpty();
        sink.close();
    }

    @Test
    void shipsRestCatalogConfigurationWithoutASerializableClient() throws Exception {
        IcebergSink<String> sink = IcebergSink.forRestCatalog("catalog", "http://localhost:8181",
                "lms.events", Map.of("s3.endpoint", "http://minio:9000"),
                value -> Map.of("value", value), "lms-events");

        assertThat(roundTrip(sink)).isInstanceOf(IcebergSink.class);
        RestCatalogTableLoader loader = roundTrip(new RestCatalogTableLoader("catalog",
                "http://localhost:8181", "lms.events", Map.of("s3.endpoint", "http://minio:9000")));
        assertThat(loader.catalogName()).isEqualTo("catalog");
        assertThat(loader.properties()).containsEntry("s3.endpoint", "http://minio:9000");
    }

    @Test
    void rejectsMapperFieldsThatDoNotMatchTheDestinationSchema() throws Exception {
        InMemoryCatalog catalog = catalog();
        TableIdentifier identifier = TableIdentifier.of("test", "mapping-errors");
        catalog.createTable(identifier, SCHEMA, PartitionSpec.unpartitioned());
        TableLoader loader = () -> catalog.loadTable(identifier);

        IcebergSink<String> unknown = new IcebergSink<>(loader,
                value -> Map.of("value", value, "typo", value), "unknown-field");
        unknown.open(new TestOperatorContext());
        assertThatThrownBy(() -> unknown.processElement(new StreamRecord<>("one", 1),
                discardingCollector()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("typo");
        unknown.close();

        IcebergSink<String> missing = new IcebergSink<>(loader, value -> Map.of(),
                "missing-field");
        missing.open(new TestOperatorContext());
        assertThatThrownBy(() -> missing.processElement(new StreamRecord<>("one", 1),
                discardingCollector()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("value");
        missing.close();
    }

    @Test
    void anEmptyCheckpointCreatesNeitherADataFileNorASnapshot() throws Exception {
        InMemoryCatalog catalog = catalog();
        TableIdentifier identifier = TableIdentifier.of("test", "empty-checkpoint");
        Table table = catalog.createTable(identifier, SCHEMA, PartitionSpec.unpartitioned());
        IcebergSink<String> sink = sink(() -> catalog.loadTable(identifier));
        sink.open(new TestOperatorContext());

        IcebergSink.IcebergSinkState state = (IcebergSink.IcebergSinkState) sink.preCommit(1);
        sink.notifyCheckpointComplete(1);

        assertThat(state.pendingFiles()).containsEntry(1L, List.of());
        assertThat(table.currentSnapshot()).isNull();
        assertThat(dataFilePaths(table)).isEmpty();
        sink.close();
    }

    private static IcebergSink<String> sink(TableLoader loader) {
        return new IcebergSink<>(loader, value -> Map.of("value", value), "test-sink");
    }

    private static InMemoryCatalog catalog() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Map.of());
        catalog.createNamespace(org.apache.iceberg.catalog.Namespace.of("test"));
        return catalog;
    }

    @SuppressWarnings("unchecked")
    private static <T> T roundTrip(T value) throws Exception {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
             ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(value);
            try (ObjectInputStream input = new ObjectInputStream(
                    new ByteArrayInputStream(bytes.toByteArray()))) {
                return (T) input.readObject();
            }
        }
    }

    private static List<String> dataFilePaths(Table table) throws Exception {
        table.refresh();
        List<String> paths = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                paths.add(task.file().location());
            }
        }
        return paths;
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
}
