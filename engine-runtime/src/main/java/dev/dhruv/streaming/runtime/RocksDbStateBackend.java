package dev.dhruv.streaming.runtime;

import dev.dhruv.streaming.api.state.ListState;
import dev.dhruv.streaming.api.state.StateBackend;
import dev.dhruv.streaming.api.state.StateHandle;
import dev.dhruv.streaming.api.state.ValueState;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * RocksDB-backed keyed state, with one column family for every named user state.
 *
 * <p>The default RocksDB column family is reserved for tiny engine metadata (the state name,
 * shape and Java type). Each state requested by an operator gets its own column family, making
 * the physical layout mirror the API: {@code sessions} and {@code pending-borrows} are visibly
 * separate stores rather than prefixes hidden in one large map. The backend stores values with
 * Java serialization because job records already have to be serializable to cross worker
 * boundaries in this teaching engine.
 *
 * <p>{@link #snapshot(long, Path)} uses RocksDB's checkpoint API. It produces a consistent local
 * directory without copying a moving database; the returned file URI is deliberately the same
 * {@link StateHandle} shape that a later MinIO checkpoint store can publish.
 */
public final class RocksDbStateBackend implements StateBackend {

    private static final byte[] DESCRIPTOR_PREFIX = "descriptor/".getBytes(StandardCharsets.UTF_8);
    private static final String DEFAULT_COLUMN_FAMILY = "default";

    private final Path databaseDirectory;
    private final DBOptions databaseOptions = new DBOptions()
            .setCreateIfMissing(true)
            .setCreateMissingColumnFamilies(true);
    private final Map<String, StateDescriptor> descriptors = new HashMap<>();
    private final Map<String, ColumnFamilyHandle> stateColumns = new HashMap<>();
    private final List<ColumnFamilyOptions> columnOptions = new ArrayList<>();

    private RocksDB database;
    private ColumnFamilyHandle metadataColumn;
    private Object currentKey;

    /**
     * Opens or creates a database in {@code databaseDirectory}. The directory is task-local;
     * checkpoint directories are written separately by {@link #snapshot(long, Path)}.
     */
    public RocksDbStateBackend(Path databaseDirectory) throws IOException {
        RocksDB.loadLibrary();
        this.databaseDirectory = Objects.requireNonNull(databaseDirectory, "databaseDirectory")
                .toAbsolutePath().normalize();
        openDatabase();
    }

    @Override
    public synchronized void setCurrentKey(Object key) {
        currentKey = Objects.requireNonNull(key, "key");
    }

    @Override
    public synchronized <T> ValueState<T> valueState(String stateName, Class<T> type) {
        ensureState(stateName, type, StateKind.VALUE);
        return new ValueStateView<>(stateName, type);
    }

    @Override
    public synchronized <T> ListState<T> listState(String stateName, Class<T> type) {
        ensureState(stateName, type, StateKind.LIST);
        return new ListStateView<>(stateName, type);
    }

    @Override
    public synchronized StateHandle snapshot(long checkpointId, Path checkpointDir) throws IOException {
        Objects.requireNonNull(checkpointDir, "checkpointDir");
        Path parent = checkpointDir.toAbsolutePath().normalize();
        Path snapshot = parent.resolve("checkpoint-" + checkpointId).normalize();
        if (snapshot.startsWith(databaseDirectory)) {
            throw new IOException("checkpoint directory must not be inside the live RocksDB directory");
        }
        if (Files.exists(snapshot)) {
            throw new IOException("checkpoint directory already exists: " + snapshot);
        }
        Files.createDirectories(parent);
        try (Checkpoint checkpoint = Checkpoint.create(database)) {
            checkpoint.createCheckpoint(snapshot.toString());
        } catch (RocksDBException e) {
            throw new IOException("could not create RocksDB checkpoint " + checkpointId, e);
        }
        return new StateHandle(snapshot.toUri(), directorySize(snapshot));
    }

    @Override
    public synchronized void restore(StateHandle handle) throws IOException {
        Objects.requireNonNull(handle, "handle");
        Path snapshot = pathFor(handle.uri());
        if (!Files.isDirectory(snapshot)) {
            throw new IOException("RocksDB state handle must point to a checkpoint directory: "
                    + snapshot);
        }
        if (snapshot.equals(databaseDirectory) || snapshot.startsWith(databaseDirectory)) {
            throw new IOException("cannot restore a RocksDB database from inside itself: " + snapshot);
        }

        closeDatabase();
        try {
            deleteDirectoryContents(databaseDirectory);
            copyDirectory(snapshot, databaseDirectory);
            openDatabase();
            currentKey = null;
        } catch (IOException e) {
            // The database is intentionally left closed after a failed replacement. Continuing
            // with a partial restore would be worse than failing the task and recovering again.
            throw e;
        }
    }

    /** Returns the number of keyed entries, useful for state-clearance assertions. */
    public synchronized int stateEntryCount() {
        int entries = 0;
        for (ColumnFamilyHandle column : stateColumns.values()) {
            try (RocksIterator iterator = database.newIterator(column)) {
                iterator.seekToFirst();
                while (iterator.isValid()) {
                    entries++;
                    iterator.next();
                }
                checkIterator(iterator);
            }
        }
        return entries;
    }

    /** Estimates live on-disk data bytes across user state column families. */
    public synchronized long estimatedStateSizeBytes() {
        long size = 0;
        try {
            for (ColumnFamilyHandle column : stateColumns.values()) {
                size += database.getLongProperty(column, "rocksdb.estimate-live-data-size");
            }
            return size;
        } catch (RocksDBException e) {
            throw new IllegalStateException("could not estimate RocksDB state size", e);
        }
    }

    @Override
    public synchronized void close() {
        closeDatabase();
        databaseOptions.close();
        currentKey = null;
    }

    private <T> void ensureState(String stateName, Class<T> type, StateKind kind) {
        validateStateName(stateName);
        Objects.requireNonNull(type, "type");
        StateDescriptor desired = new StateDescriptor(kind, type.getName());
        StateDescriptor existing = descriptors.get(stateName);
        if (existing != null && !existing.equals(desired)) {
            throw new IllegalArgumentException("state '" + stateName + "' was requested as "
                    + existing + " and cannot also be " + desired);
        }
        if (existing == null) {
            try {
                ColumnFamilyOptions options = new ColumnFamilyOptions();
                ColumnFamilyHandle column = database.createColumnFamily(new ColumnFamilyDescriptor(
                        stateName.getBytes(StandardCharsets.UTF_8), options));
                columnOptions.add(options);
                stateColumns.put(stateName, column);
                database.put(metadataColumn, descriptorKey(stateName), descriptorBytes(desired));
                descriptors.put(stateName, desired);
            } catch (RocksDBException e) {
                throw new IllegalStateException("could not create state '" + stateName + "'", e);
            }
        }
    }

    private Object key() {
        if (currentKey == null) {
            throw new IllegalStateException("state accessed before the runtime set a current key");
        }
        return currentKey;
    }

    private <T> Optional<T> value(String stateName, Class<T> type) throws IOException {
        try {
            byte[] bytes = database.get(stateColumns.get(stateName), bytesFor(key(), "state key"));
            return bytes == null ? Optional.empty() : Optional.of(type.cast(fromBytes(bytes)));
        } catch (RocksDBException e) {
            throw new IOException("could not read value state '" + stateName + "'", e);
        }
    }

    private <T> void updateValue(String stateName, T value) throws IOException {
        Objects.requireNonNull(value, "value");
        try {
            database.put(stateColumns.get(stateName), bytesFor(key(), "state key"),
                    bytesFor(value, "state value"));
        } catch (RocksDBException e) {
            throw new IOException("could not write value state '" + stateName + "'", e);
        }
    }

    private void clearValue(String stateName) {
        delete(stateName);
    }

    private <T> Iterable<T> list(String stateName, Class<T> type) throws IOException {
        try {
            byte[] bytes = database.get(stateColumns.get(stateName), bytesFor(key(), "state key"));
            if (bytes == null) {
                return List.of();
            }
            Object decoded = fromBytes(bytes);
            if (!(decoded instanceof List<?> values)) {
                throw new IOException("list state '" + stateName + "' does not contain a list");
            }
            List<T> typed = new ArrayList<>(values.size());
            for (Object value : values) {
                typed.add(type.cast(value));
            }
            return List.copyOf(typed);
        } catch (RocksDBException e) {
            throw new IOException("could not read list state '" + stateName + "'", e);
        }
    }

    private <T> void addToList(String stateName, Class<T> type, T value) throws IOException {
        Objects.requireNonNull(value, "value");
        List<T> values = new ArrayList<>();
        for (T oldValue : list(stateName, type)) {
            values.add(oldValue);
        }
        values.add(type.cast(value));
        updateList(stateName, type, values);
    }

    private <T> void updateList(String stateName, Class<T> type, List<T> values) throws IOException {
        Objects.requireNonNull(values, "values");
        List<T> replacement = new ArrayList<>(values.size());
        for (T value : values) {
            replacement.add(type.cast(Objects.requireNonNull(value, "list element")));
        }
        if (replacement.isEmpty()) {
            delete(stateName);
            return;
        }
        try {
            database.put(stateColumns.get(stateName), bytesFor(key(), "state key"),
                    bytesFor((Serializable) replacement, "list state value"));
        } catch (ClassCastException e) {
            throw new IOException("list state '" + stateName + "' contains a non-serializable value", e);
        } catch (RocksDBException e) {
            throw new IOException("could not write list state '" + stateName + "'", e);
        }
    }

    private void delete(String stateName) {
        try {
            database.delete(stateColumns.get(stateName), bytesFor(key(), "state key"));
        } catch (IOException | RocksDBException e) {
            throw new IllegalStateException("could not clear state '" + stateName + "'", e);
        }
    }

    private void openDatabase() throws IOException {
        try {
            Files.createDirectories(databaseDirectory);
            List<byte[]> names = existingColumnFamilies();
            List<ColumnFamilyDescriptor> columnDescriptors = new ArrayList<>(names.size());
            for (byte[] name : names) {
                ColumnFamilyOptions options = new ColumnFamilyOptions();
                columnOptions.add(options);
                columnDescriptors.add(new ColumnFamilyDescriptor(name, options));
            }
            List<ColumnFamilyHandle> handles = new ArrayList<>(names.size());
            database = RocksDB.open(databaseOptions, databaseDirectory.toString(),
                    columnDescriptors, handles);
            metadataColumn = handles.getFirst();
            for (int index = 1; index < names.size(); index++) {
                stateColumns.put(new String(names.get(index), StandardCharsets.UTF_8), handles.get(index));
            }
            loadDescriptors();
        } catch (RocksDBException e) {
            throw new IOException("could not open RocksDB state directory " + databaseDirectory, e);
        }
    }

    private List<byte[]> existingColumnFamilies() throws RocksDBException {
        if (!Files.exists(databaseDirectory.resolve("CURRENT"))) {
            return List.of(RocksDB.DEFAULT_COLUMN_FAMILY);
        }
        try (Options options = new Options()) {
            return RocksDB.listColumnFamilies(options, databaseDirectory.toString());
        }
    }

    private void loadDescriptors() throws IOException {
        descriptors.clear();
        try (RocksIterator iterator = database.newIterator(metadataColumn)) {
            iterator.seek(DESCRIPTOR_PREFIX);
            while (iterator.isValid() && startsWith(iterator.key(), DESCRIPTOR_PREFIX)) {
                String stateName = new String(iterator.key(), DESCRIPTOR_PREFIX.length,
                        iterator.key().length - DESCRIPTOR_PREFIX.length, StandardCharsets.UTF_8);
                descriptors.put(stateName, descriptorFromBytes(iterator.value()));
                iterator.next();
            }
            checkIterator(iterator);
        }
        if (!stateColumns.keySet().equals(descriptors.keySet())) {
            throw new IOException("RocksDB state column families do not match their metadata");
        }
    }

    private void closeDatabase() {
        for (ColumnFamilyHandle column : stateColumns.values()) {
            column.close();
        }
        stateColumns.clear();
        if (metadataColumn != null) {
            metadataColumn.close();
            metadataColumn = null;
        }
        if (database != null) {
            database.close();
            database = null;
        }
        for (ColumnFamilyOptions options : columnOptions) {
            options.close();
        }
        columnOptions.clear();
        descriptors.clear();
    }

    private static byte[] bytesFor(Object value, String role) throws IOException {
        if (!(value instanceof Serializable serializable)) {
            throw new IOException(role + " must implement Serializable for RocksDB state: "
                    + value.getClass().getName());
        }
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
             ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(serializable);
            return bytes.toByteArray();
        }
    }

    private static Object fromBytes(byte[] bytes) throws IOException {
        try (UserCodeObjectInputStream input =
                     new UserCodeObjectInputStream(new ByteArrayInputStream(bytes))) {
            return input.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("could not load a class stored in RocksDB state", e);
        }
    }

    private static byte[] descriptorKey(String stateName) {
        return (new String(DESCRIPTOR_PREFIX, StandardCharsets.UTF_8) + stateName)
                .getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] descriptorBytes(StateDescriptor descriptor) {
        return (descriptor.kind().name() + "\n" + descriptor.typeName())
                .getBytes(StandardCharsets.UTF_8);
    }

    private static StateDescriptor descriptorFromBytes(byte[] bytes) throws IOException {
        String[] parts = new String(bytes, StandardCharsets.UTF_8).split("\\n", 2);
        if (parts.length != 2) {
            throw new IOException("invalid RocksDB state descriptor");
        }
        try {
            return new StateDescriptor(StateKind.valueOf(parts[0]), parts[1]);
        } catch (IllegalArgumentException e) {
            throw new IOException("invalid RocksDB state descriptor", e);
        }
    }

    private static void validateStateName(String stateName) {
        Objects.requireNonNull(stateName, "stateName");
        if (stateName.isBlank() || DEFAULT_COLUMN_FAMILY.equals(stateName)
                || stateName.startsWith("descriptor/")) {
            throw new IllegalArgumentException("state name is reserved or blank: " + stateName);
        }
    }

    private static void checkIterator(RocksIterator iterator) {
        try {
            iterator.status();
        } catch (RocksDBException e) {
            throw new IllegalStateException("could not iterate RocksDB state", e);
        }
    }

    private static boolean startsWith(byte[] value, byte[] prefix) {
        return value.length >= prefix.length
                && Arrays.equals(Arrays.copyOf(value, prefix.length), prefix);
    }

    private static Path pathFor(URI uri) throws IOException {
        if (!"file".equalsIgnoreCase(uri.getScheme())) {
            throw new IOException("RocksDB state handles must use file URIs: " + uri);
        }
        return Path.of(uri).toAbsolutePath().normalize();
    }

    private static long directorySize(Path directory) throws IOException {
        try (var paths = Files.walk(directory)) {
            return paths.filter(Files::isRegularFile).mapToLong(path -> {
                try {
                    return Files.size(path);
                } catch (IOException e) {
                    throw new DirectorySizeException(e);
                }
            }).sum();
        } catch (DirectorySizeException e) {
            throw e.cause;
        }
    }

    private static void deleteDirectoryContents(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            Files.createDirectories(directory);
            return;
        }
        try (var paths = Files.list(directory)) {
            for (Path child : paths.toList()) {
                Files.walkFileTree(child, new DeletingVisitor());
            }
        }
    }

    private static void copyDirectory(Path source, Path destination) throws IOException {
        Files.walkFileTree(source, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path directory, BasicFileAttributes attributes)
                    throws IOException {
                Files.createDirectories(destination.resolve(source.relativize(directory)));
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attributes)
                    throws IOException {
                Files.copy(file, destination.resolve(source.relativize(file)));
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private enum StateKind { VALUE, LIST }

    private record StateDescriptor(StateKind kind, String typeName) {
    }

    private static final class DirectorySizeException extends RuntimeException {
        private final IOException cause;

        private DirectorySizeException(IOException cause) {
            this.cause = cause;
        }
    }

    private static final class DeletingVisitor extends SimpleFileVisitor<Path> {
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException {
            Files.delete(file);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path directory, IOException failure) throws IOException {
            if (failure != null) {
                throw failure;
            }
            Files.delete(directory);
            return FileVisitResult.CONTINUE;
        }
    }

    private final class ValueStateView<T> implements ValueState<T> {
        private final String stateName;
        private final Class<T> type;

        private ValueStateView(String stateName, Class<T> type) {
            this.stateName = stateName;
            this.type = type;
        }

        @Override
        public Optional<T> value() throws IOException {
            return RocksDbStateBackend.this.value(stateName, type);
        }

        @Override
        public void update(T value) throws IOException {
            RocksDbStateBackend.this.updateValue(stateName, value);
        }

        @Override
        public void clear() {
            RocksDbStateBackend.this.clearValue(stateName);
        }
    }

    private final class ListStateView<T> implements ListState<T> {
        private final String stateName;
        private final Class<T> type;

        private ListStateView(String stateName, Class<T> type) {
            this.stateName = stateName;
            this.type = type;
        }

        @Override
        public Iterable<T> get() throws IOException {
            return RocksDbStateBackend.this.list(stateName, type);
        }

        @Override
        public void add(T value) throws IOException {
            RocksDbStateBackend.this.addToList(stateName, type, value);
        }

        @Override
        public void update(List<T> values) throws IOException {
            RocksDbStateBackend.this.updateList(stateName, type, values);
        }

        @Override
        public void clear() {
            RocksDbStateBackend.this.delete(stateName);
        }
    }
}
