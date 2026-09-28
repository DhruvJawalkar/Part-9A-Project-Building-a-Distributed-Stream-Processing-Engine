package dev.dhruv.streaming.connectors.iceberg;

import dev.dhruv.streaming.api.CheckpointListener;
import dev.dhruv.streaming.api.Collector;
import dev.dhruv.streaming.api.Operator;
import dev.dhruv.streaming.api.OperatorContext;
import dev.dhruv.streaming.api.StreamRecord;
import dev.dhruv.streaming.api.metrics.Counter;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * A checkpoint-transactional sink for an unpartitioned Apache Iceberg table.
 *
 * <p>Records are written to a private Parquet file for the current checkpoint interval.  A
 * checkpoint calls {@link #preCommit(long)}, which closes that file and returns serializable
 * pending-file state.  The file becomes visible only when the coordinator later calls
 * {@link #notifyCheckpointComplete(long)}: one Iceberg append commit publishes every pending
 * file for that interval.  Thus files from an incomplete or aborted interval are harmless
 * storage orphans, never table rows.
 *
 * <p>The runtime owns the ordering of checkpoint callbacks.  This class intentionally owns no
 * coordinator transport; implementing {@link CheckpointListener} is the narrow connection to
 * the operator checkpoint protocol.
 *
 * @param <T> stream value type
 */
public final class IcebergSink<T> implements Operator<T, Void>, CheckpointListener {

    private static final long serialVersionUID = 1L;

    private final TableLoader tableLoader;
    private final RecordMapper<T> recordMapper;
    private final String sinkId;

    /** Durable operator state returned at every pre-commit. */
    private final Map<Long, List<PendingDataFile>> pendingFiles = new HashMap<>();

    private transient Table table;
    private transient Schema schema;
    private transient DataWriter<Record> activeWriter;
    private transient OutputFile activeOutputFile;
    private transient long activeRecordCount;
    private transient Counter recordsWritten;

    /**
     * Creates a sink. The schema must match the current schema of an unpartitioned destination
     * table returned by {@code tableLoader}.
     */
    IcebergSink(TableLoader tableLoader, RecordMapper<T> recordMapper, String sinkId) {
        this.tableLoader = Objects.requireNonNull(tableLoader, "tableLoader");
        this.recordMapper = Objects.requireNonNull(recordMapper, "recordMapper");
        if (sinkId == null || sinkId.isBlank()) {
            throw new IllegalArgumentException("sinkId must not be blank");
        }
        this.sinkId = sinkId.replaceAll("[^A-Za-z0-9._-]", "_");
    }

    /**
     * Creates a worker-serializable sink for an Iceberg REST catalog. No catalog or S3 client
     * is captured in the submitted job; those resources are opened by the worker.
     */
    public static <T> IcebergSink<T> forRestCatalog(String catalogName, String catalogUri,
            String tableName, Map<String, String> properties, RecordMapper<T> recordMapper,
            String sinkId) {
        return new IcebergSink<>(new RestCatalogTableLoader(catalogName, catalogUri, tableName,
                properties), recordMapper, sinkId);
    }

    @Override
    public void open(OperatorContext ctx) throws Exception {
        table = tableLoader.load();
        schema = table.schema();
        if (!table.spec().isUnpartitioned()) {
            throw new IllegalArgumentException("IcebergSink currently requires an unpartitioned table");
        }
        recordsWritten = ctx.metrics().counter("records-written");
        openNextWriter();
    }

    @Override
    public void processElement(StreamRecord<T> record, Collector<Void> out) throws Exception {
        ensureOpen();
        activeWriter.write(toIcebergRecord(record.value()));
        activeRecordCount++;
        recordsWritten.increment();
    }

    /**
     * Closes the current interval's Parquet writer and captures every uncommitted file path in
     * serializable operator state. Repeating a call for an already prepared checkpoint is safe.
     */
    @Override
    public Serializable preCommit(long checkpointId) throws Exception {
        ensureOpen();
        if (pendingFiles.containsKey(checkpointId)) {
            return snapshotState();
        }

        PendingDataFile closed = closeCurrentWriter();
        pendingFiles.put(checkpointId, closed == null ? List.of() : List.of(closed));
        openNextWriter();
        return snapshotState();
    }

    /** Returns a copy of the state that is embedded in the operator checkpoint envelope. */
    public IcebergSinkState snapshotState() {
        return new IcebergSinkState(pendingFiles);
    }

    /**
     * Restores file paths prepared before a task failure. The next completion notification can
     * then publish them, and retrying that notification remains idempotent.
     */
    @Override
    public void restoreCheckpointState(Serializable state) {
        if (state == null) {
            pendingFiles.clear();
            return;
        }
        if (!(state instanceof IcebergSinkState restored)) {
            throw new IllegalArgumentException("IcebergSink received incompatible checkpoint state: "
                    + state.getClass().getName());
        }
        pendingFiles.clear();
        pendingFiles.putAll(restored.pendingFiles());
    }

    /**
     * Makes one checkpoint interval visible with a single Iceberg metadata commit.
     *
     * <p>Before committing, the current table snapshot is scanned for the pending paths. This
     * covers a crash after a successful commit but before the completion acknowledgement, so a
     * duplicate notification never appends the same data file twice.
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        ensureOpen();
        List<PendingDataFile> pending = pendingFiles.get(checkpointId);
        if (pending == null || pending.isEmpty()) {
            pendingFiles.remove(checkpointId);
            return;
        }

        Set<String> visiblePaths = visibleDataPaths();
        List<PendingDataFile> missing = pending.stream()
                .filter(file -> !visiblePaths.contains(file.path()))
                .toList();
        if (!missing.isEmpty()) {
            AppendFiles append = table.newAppend();
            for (PendingDataFile file : missing) {
                append.appendFile(toDataFile(file));
            }
            append.commit();
        }
        pendingFiles.remove(checkpointId);
    }

    /** Drops aborted checkpoint metadata; its already-written files stay unreferenced. */
    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        pendingFiles.remove(checkpointId);
    }

    @Override
    public void close() throws Exception {
        if (activeWriter != null) {
            DataWriter<Record> writer = activeWriter;
            OutputFile output = activeOutputFile;
            activeWriter = null;
            activeOutputFile = null;
            writer.close();
            // A normal shutdown immediately after a completed checkpoint should not leave an
            // empty object behind. A non-empty, not-yet-prepared file is deliberately retained:
            // after worker loss it is an orphan for Iceberg maintenance to remove, never data
            // visible through a table snapshot.
            if (activeRecordCount == 0 && output != null) {
                deleteIfExists(output);
            }
        }
    }

    private void openNextWriter() throws Exception {
        String location = table.location() + "/data/" + sinkId + "/" + UUID.randomUUID() + ".parquet";
        activeOutputFile = table.io().newOutputFile(location);
        activeWriter = Parquet.writeData(activeOutputFile)
                .forTable(table)
                .schema(schema)
                .createWriterFunc(GenericParquetWriter::create)
                .build();
        activeRecordCount = 0;
    }

    private Record toIcebergRecord(T value) {
        Map<String, ?> fields = Objects.requireNonNull(recordMapper.map(value),
                "RecordMapper returned null");
        Set<String> tableFields = schema.columns().stream()
                .map(org.apache.iceberg.types.Types.NestedField::name)
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
        Set<String> unknownFields = new HashSet<>(fields.keySet());
        unknownFields.removeAll(tableFields);
        if (!unknownFields.isEmpty()) {
            throw new IllegalArgumentException("record mapper produced fields absent from table '"
                    + table.name() + "': " + unknownFields);
        }

        GenericRecord record = GenericRecord.create(schema);
        for (org.apache.iceberg.types.Types.NestedField field : schema.columns()) {
            if (field.isRequired() && (!fields.containsKey(field.name())
                    || fields.get(field.name()) == null)) {
                throw new IllegalArgumentException("record mapper omitted required table field '"
                        + field.name() + "'");
            }
            if (fields.containsKey(field.name())) {
                record.setField(field.name(), fields.get(field.name()));
            }
        }
        return record;
    }

    private PendingDataFile closeCurrentWriter() throws Exception {
        DataWriter<Record> writer = activeWriter;
        OutputFile output = activeOutputFile;
        activeWriter = null;
        activeOutputFile = null;
        writer.close();
        if (activeRecordCount == 0) {
            deleteIfExists(output);
            return null;
        }
        DataFile dataFile = writer.toDataFile();
        return new PendingDataFile(dataFile.location(), dataFile.fileSizeInBytes(),
                dataFile.recordCount());
    }

    private DataFile toDataFile(PendingDataFile file) {
        return DataFiles.builder(table.spec())
                .withPath(file.path())
                .withFileSizeInBytes(file.fileSizeBytes())
                .withRecordCount(file.recordCount())
                .build();
    }

    private Set<String> visibleDataPaths() throws Exception {
        // Another attempt or sink subtask may have committed through a different Table object.
        // Refresh before the idempotence check so a replay never appends a path already visible
        // in the current table state.
        table.refresh();
        Set<String> paths = new HashSet<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                paths.add(task.file().location());
            }
        }
        return paths;
    }

    private void deleteIfExists(OutputFile output) {
        // Some FileIO implementations materialize an empty object only on the first write;
        // closing an untouched Parquet writer can therefore leave nothing to delete.
        try {
            table.io().deleteFile(output.location());
        } catch (org.apache.iceberg.exceptions.NotFoundException ignored) {
            // Already absent is the desired postcondition.
        }
    }

    private void ensureOpen() {
        if (table == null || activeWriter == null) {
            throw new IllegalStateException("IcebergSink is not open");
        }
    }

    /** Serializable state embedded in a task checkpoint. */
    public record IcebergSinkState(Map<Long, List<PendingDataFile>> pendingFiles)
            implements Serializable {
        private static final long serialVersionUID = 1L;

        public IcebergSinkState {
            Map<Long, List<PendingDataFile>> copy = new HashMap<>();
            pendingFiles.forEach((checkpointId, files) ->
                    copy.put(checkpointId, List.copyOf(files)));
            pendingFiles = Map.copyOf(copy);
        }
    }

    /** Metadata sufficient to reconstruct the unpartitioned Iceberg data-file descriptor. */
    public record PendingDataFile(String path, long fileSizeBytes, long recordCount)
            implements Serializable {
        private static final long serialVersionUID = 1L;

        public PendingDataFile {
            Objects.requireNonNull(path, "path");
            if (fileSizeBytes < 0 || recordCount < 0) {
                throw new IllegalArgumentException("file size and record count must be non-negative");
            }
        }
    }
}
