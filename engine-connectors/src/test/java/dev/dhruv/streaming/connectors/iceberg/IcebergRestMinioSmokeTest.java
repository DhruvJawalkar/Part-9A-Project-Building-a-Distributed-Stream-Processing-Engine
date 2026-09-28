package dev.dhruv.streaming.connectors.iceberg;

import dev.dhruv.streaming.api.Collector;
import dev.dhruv.streaming.api.OperatorContext;
import dev.dhruv.streaming.api.StreamRecord;
import dev.dhruv.streaming.api.metrics.Counter;
import dev.dhruv.streaming.api.metrics.Histogram;
import dev.dhruv.streaming.api.metrics.MetricGroup;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.LongSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Opt-in smoke proof for the real REST-catalog/S3 path used by the Compose demo.
 *
 * <p>The deterministic acceptance tests deliberately use local files. This test is kept out of
 * ordinary unit runs because it expects {@code minio}, {@code minio-init}, and
 * {@code iceberg-rest} from {@code docker-compose.yml} plus the bootstrapped demo schemas.
 */
@EnabledIfSystemProperty(named = "icebergRestSmoke", matches = "true")
class IcebergRestMinioSmokeTest {

    private static final String TABLE = "lms.analytics.click_conversions";
    private static final Map<String, String> PROPERTIES = Map.of(
            "warehouse", "s3://warehouse/",
            "io-impl", "org.apache.iceberg.aws.s3.S3FileIO",
            "s3.endpoint", "http://localhost:9000",
            "s3.path-style-access", "true",
            "s3.access-key-id", "minioadmin",
            "s3.secret-access-key", "minioadmin",
            "client.region", "us-east-1");

    @Test
    void writesParquetToMinioAndPublishesItThroughTheRestCatalog() throws Exception {
        RestCatalogTableLoader loader = new RestCatalogTableLoader("lms",
                "http://localhost:8181", TABLE, PROPERTIES);
        Table before = loader.load();
        Set<String> originalPaths = dataFilePaths(before);
        long originalSnapshots = snapshotCount(before);
        String marker = "smoke-" + UUID.randomUUID();

        IcebergSink<Map<String, ?>> sink = IcebergSink.forRestCatalog("lms",
                "http://localhost:8181", TABLE, PROPERTIES, value -> value,
                "rest-minio-smoke");
        sink.open(new SmokeContext());
        LocalDateTime clickTime = LocalDateTime.ofEpochSecond(1_700_000_000L, 0,
                ZoneOffset.UTC);
        sink.processElement(new StreamRecord<>(Map.of(
                "member_id", marker,
                "catalog_item_id", "book-1",
                "search_term", "distributed systems",
                "loan_id", "loan-1",
                "click_time", clickTime,
                "borrow_time", clickTime.plusSeconds(2),
                "conversion_delay_ms", 2_000L), 1_700_000_000_000L), discardingCollector());

        long checkpointId = 9_000_000L;
        IcebergSink.IcebergSinkState prepared =
                (IcebergSink.IcebergSinkState) sink.preCommit(checkpointId);
        String pendingPath = prepared.pendingFiles().get(checkpointId).getFirst().path();

        assertThat(dataFilePaths(loader.load())).doesNotContain(pendingPath);
        sink.notifyCheckpointComplete(checkpointId);

        Table committed = loader.load();
        assertThat(dataFilePaths(committed))
                .contains(pendingPath)
                .containsAll(originalPaths);
        assertThat(pendingPath).startsWith("s3://warehouse/");
        assertThat(snapshotCount(committed)).isEqualTo(originalSnapshots + 1);
        sink.close();
    }

    private static Set<String> dataFilePaths(Table table) throws Exception {
        table.refresh();
        Set<String> paths = new HashSet<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                paths.add(task.file().location());
            }
        }
        return paths;
    }

    private static long snapshotCount(Table table) {
        long count = 0;
        for (org.apache.iceberg.Snapshot ignored : table.snapshots()) {
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

    private record SmokeContext() implements OperatorContext {
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
