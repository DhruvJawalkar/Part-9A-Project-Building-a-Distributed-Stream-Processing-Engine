package dev.dhruv.streaming.runtime;

import dev.dhruv.streaming.api.state.ListState;
import dev.dhruv.streaming.api.state.StateBackend;
import dev.dhruv.streaming.api.state.StateHandle;
import dev.dhruv.streaming.api.state.ValueState;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Heap-backed keyed state for the early phases of the teaching engine.
 *
 * <p>State handles returned by this backend are views, not snapshots: they resolve the current
 * key at every operation. That is what lets an operator retain a handle from {@code open()} and
 * still see a different value for every record and timer callback.
 */
public final class InMemoryStateBackend implements StateBackend {

    private final Map<String, Map<Object, Object>> valueStates = new HashMap<>();
    private final Map<String, Map<Object, List<Object>>> listStates = new HashMap<>();
    private final Map<String, StateDescriptor> descriptors = new HashMap<>();
    private Object currentKey;

    @Override
    public synchronized void setCurrentKey(Object key) {
        currentKey = Objects.requireNonNull(key, "key");
    }

    @Override
    public synchronized <T> ValueState<T> valueState(String stateName, Class<T> type) {
        validateDescriptor(stateName, type, StateKind.VALUE);
        valueStates.computeIfAbsent(stateName, ignored -> new HashMap<>());
        return new ValueStateView<>(stateName, type);
    }

    @Override
    public synchronized <T> ListState<T> listState(String stateName, Class<T> type) {
        validateDescriptor(stateName, type, StateKind.LIST);
        listStates.computeIfAbsent(stateName, ignored -> new HashMap<>());
        return new ListStateView<>(stateName, type);
    }

    @Override
    public synchronized StateHandle snapshot(long checkpointId, Path checkpointDir) throws IOException {
        Objects.requireNonNull(checkpointDir, "checkpointDir");
        Files.createDirectories(checkpointDir);
        Path snapshot = checkpointDir.resolve("checkpoint-" + checkpointId + ".bin");
        try (ObjectOutputStream output = new ObjectOutputStream(Files.newOutputStream(snapshot))) {
            output.writeObject(new Snapshot(copyValueStates(), copyListStates(), new HashMap<>(descriptors)));
        }
        return new StateHandle(snapshot.toAbsolutePath().toUri(), Files.size(snapshot));
    }

    @Override
    public synchronized void restore(StateHandle handle) throws IOException {
        Objects.requireNonNull(handle, "handle");
        Path snapshot = pathFor(handle.uri());
        try (ObjectInputStream input = new ObjectInputStream(Files.newInputStream(snapshot))) {
            Object restored = input.readObject();
            if (!(restored instanceof Snapshot state)) {
                throw new IOException("state handle does not contain an in-memory state snapshot");
            }
            valueStates.clear();
            valueStates.putAll(state.valueStates());
            listStates.clear();
            listStates.putAll(state.listStates());
            descriptors.clear();
            descriptors.putAll(state.descriptors());
            currentKey = null;
        } catch (ClassNotFoundException e) {
            throw new IOException("could not read state snapshot", e);
        }
    }

    /** Returns how many keyed state entries are currently retained, for leak-focused tests. */
    public synchronized int stateEntryCount() {
        return valueStates.values().stream().mapToInt(Map::size).sum()
                + listStates.values().stream().mapToInt(Map::size).sum();
    }

    @Override
    public synchronized void close() {
        valueStates.clear();
        listStates.clear();
        descriptors.clear();
        currentKey = null;
    }

    private void validateDescriptor(String stateName, Class<?> type, StateKind kind) {
        Objects.requireNonNull(stateName, "stateName");
        Objects.requireNonNull(type, "type");
        StateDescriptor wanted = new StateDescriptor(kind, type.getName());
        StateDescriptor existing = descriptors.putIfAbsent(stateName, wanted);
        if (existing != null && !existing.equals(wanted)) {
            throw new IllegalArgumentException("state '" + stateName + "' was requested as "
                    + existing + " and cannot also be " + wanted);
        }
    }

    private Object key() {
        if (currentKey == null) {
            throw new IllegalStateException("state accessed before the runtime set a current key");
        }
        return currentKey;
    }

    @SuppressWarnings("unchecked")
    private synchronized <T> Optional<T> value(String stateName, Class<T> type) {
        Object value = valueStates.get(stateName).get(key());
        return Optional.ofNullable(value).map(type::cast);
    }

    private synchronized <T> void updateValue(String stateName, Class<T> type, T value) {
        valueStates.get(stateName).put(key(), type.cast(Objects.requireNonNull(value, "value")));
    }

    private synchronized void clearValue(String stateName) {
        valueStates.get(stateName).remove(key());
    }

    @SuppressWarnings("unchecked")
    private synchronized <T> Iterable<T> list(String stateName, Class<T> type) {
        List<Object> values = listStates.get(stateName).get(key());
        if (values == null) {
            return List.of();
        }
        List<T> typed = new ArrayList<>(values.size());
        for (Object value : values) {
            typed.add(type.cast(value));
        }
        return List.copyOf(typed);
    }

    private synchronized <T> void addToList(String stateName, Class<T> type, T value) {
        listStates.get(stateName).computeIfAbsent(key(), ignored -> new ArrayList<>())
                .add(type.cast(Objects.requireNonNull(value, "value")));
    }

    private synchronized <T> void updateList(String stateName, Class<T> type, List<T> values) {
        Objects.requireNonNull(values, "values");
        List<Object> replacement = new ArrayList<>(values.size());
        for (T value : values) {
            replacement.add(type.cast(Objects.requireNonNull(value, "list element")));
        }
        if (replacement.isEmpty()) {
            listStates.get(stateName).remove(key());
        } else {
            listStates.get(stateName).put(key(), replacement);
        }
    }

    private synchronized void clearList(String stateName) {
        listStates.get(stateName).remove(key());
    }

    private Map<String, Map<Object, Object>> copyValueStates() {
        Map<String, Map<Object, Object>> copied = new HashMap<>();
        valueStates.forEach((name, values) -> copied.put(name, new HashMap<>(values)));
        return copied;
    }

    private Map<String, Map<Object, List<Object>>> copyListStates() {
        Map<String, Map<Object, List<Object>>> copied = new HashMap<>();
        listStates.forEach((name, values) -> {
            Map<Object, List<Object>> copiedValues = new HashMap<>();
            values.forEach((key, list) -> copiedValues.put(key, new ArrayList<>(list)));
            copied.put(name, copiedValues);
        });
        return copied;
    }

    private static Path pathFor(URI uri) throws IOException {
        if (!"file".equalsIgnoreCase(uri.getScheme())) {
            throw new IOException("in-memory state handles must use file URIs: " + uri);
        }
        return Path.of(uri);
    }

    private enum StateKind { VALUE, LIST }

    private record StateDescriptor(StateKind kind, String typeName) implements Serializable { }

    private record Snapshot(Map<String, Map<Object, Object>> valueStates,
                            Map<String, Map<Object, List<Object>>> listStates,
                            Map<String, StateDescriptor> descriptors) implements Serializable { }

    private final class ValueStateView<T> implements ValueState<T> {
        private final String stateName;
        private final Class<T> type;

        private ValueStateView(String stateName, Class<T> type) {
            this.stateName = stateName;
            this.type = type;
        }

        @Override
        public Optional<T> value() {
            return InMemoryStateBackend.this.value(stateName, type);
        }

        @Override
        public void update(T value) {
            InMemoryStateBackend.this.updateValue(stateName, type, value);
        }

        @Override
        public void clear() {
            InMemoryStateBackend.this.clearValue(stateName);
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
        public Iterable<T> get() {
            return InMemoryStateBackend.this.list(stateName, type);
        }

        @Override
        public void add(T value) {
            InMemoryStateBackend.this.addToList(stateName, type, value);
        }

        @Override
        public void update(List<T> values) {
            InMemoryStateBackend.this.updateList(stateName, type, values);
        }

        @Override
        public void clear() {
            InMemoryStateBackend.this.clearList(stateName);
        }
    }
}
