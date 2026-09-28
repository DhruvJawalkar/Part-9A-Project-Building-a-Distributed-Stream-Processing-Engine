package dev.dhruv.streaming.runtime;

import dev.dhruv.streaming.api.state.ListState;
import dev.dhruv.streaming.api.state.StateHandle;
import dev.dhruv.streaming.api.state.ValueState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RocksDbStateBackendTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void keepsValueAndListStateSeparateAndScopedToTheCurrentKey() throws Exception {
        try (RocksDbStateBackend backend = backend("live")) {
            ValueState<Integer> count = backend.valueState("count", Integer.class);
            ListState<String> terms = backend.listState("terms", String.class);

            backend.setCurrentKey("member-a");
            count.update(2);
            terms.add("distributed");
            terms.add("systems");
            backend.setCurrentKey("member-b");
            count.update(1);
            terms.add("streaming");

            assertThat(count.value()).contains(1);
            assertThat(terms.get()).containsExactly("streaming");
            backend.setCurrentKey("member-a");
            assertThat(count.value()).contains(2);
            assertThat(terms.get()).containsExactly("distributed", "systems");
            assertThat(backend.stateEntryCount()).isEqualTo(4);

            count.clear();
            terms.clear();
            assertThat(backend.stateEntryCount()).isEqualTo(2);
        }
    }

    @Test
    void snapshotAndRestoreReplaceAllColumnFamiliesRatherThanMerging(@TempDir Path checkpoints)
            throws Exception {
        try (RocksDbStateBackend backend = backend("live")) {
            ValueState<Integer> count = backend.valueState("count", Integer.class);
            ListState<String> terms = backend.listState("terms", String.class);
            backend.setCurrentKey("member-a");
            count.update(2);
            terms.update(List.of("one", "two"));
            StateHandle handle = backend.snapshot(7, checkpoints);

            assertThat(handle.uri().getScheme()).isEqualTo("file");
            assertThat(handle.sizeBytes()).isPositive();
            assertThat(Files.isDirectory(Path.of(handle.uri()))).isTrue();

            backend.setCurrentKey("member-a");
            count.update(99);
            backend.setCurrentKey("member-b");
            count.update(5);
            terms.add("new");
            backend.restore(handle);

            backend.setCurrentKey("member-a");
            assertThat(count.value()).contains(2);
            assertThat(terms.get()).containsExactly("one", "two");
            backend.setCurrentKey("member-b");
            assertThat(count.value()).isEmpty();
            assertThat(terms.get()).isEmpty();
        }
    }

    @Test
    void persistedDescriptorsPreventStateNameShapeChangesAcrossAReopen() throws Exception {
        Path live = temporaryDirectory.resolve("live");
        try (RocksDbStateBackend first = new RocksDbStateBackend(live)) {
            first.setCurrentKey("member");
            first.valueState("session", String.class).update("open");
        }

        try (RocksDbStateBackend reopened = new RocksDbStateBackend(live)) {
            reopened.setCurrentKey("member");
            assertThat(reopened.valueState("session", String.class).value()).contains("open");
            assertThatThrownBy(() -> reopened.listState("session", String.class))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("session");
        }
    }

    @Test
    void rejectsStateAccessBeforeTheRuntimeEstablishesAKey() throws Exception {
        try (RocksDbStateBackend backend = backend("live")) {
            ValueState<Integer> count = backend.valueState("count", Integer.class);
            assertThatThrownBy(count::value)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("current key");
        }
    }

    private RocksDbStateBackend backend(String directory) throws Exception {
        return new RocksDbStateBackend(temporaryDirectory.resolve(directory));
    }
}
