package dev.dhruv.streaming.runtime;

import dev.dhruv.streaming.api.state.ListState;
import dev.dhruv.streaming.api.state.StateHandle;
import dev.dhruv.streaming.api.state.ValueState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InMemoryStateBackendTest {

    @Test
    void retainedHandlesAreScopedToWhicheverKeyTheRuntimeMakesCurrent() throws Exception {
        InMemoryStateBackend backend = new InMemoryStateBackend();
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
    }

    @Test
    void clearReleasesTheKeyedEntries() throws Exception {
        InMemoryStateBackend backend = new InMemoryStateBackend();
        ValueState<Integer> count = backend.valueState("count", Integer.class);
        ListState<String> terms = backend.listState("terms", String.class);
        backend.setCurrentKey("member");
        count.update(2);
        terms.add("term");

        count.clear();
        terms.clear();

        assertThat(count.value()).isEmpty();
        assertThat(terms.get()).isEmpty();
        assertThat(backend.stateEntryCount()).isZero();
    }

    @Test
    void snapshotAndRestoreReplaceRatherThanMergeState(@TempDir Path checkpointDir) throws Exception {
        InMemoryStateBackend backend = new InMemoryStateBackend();
        ValueState<Integer> count = backend.valueState("count", Integer.class);
        ListState<String> terms = backend.listState("terms", String.class);
        backend.setCurrentKey("member-a");
        count.update(2);
        terms.update(List.of("one", "two"));
        StateHandle handle = backend.snapshot(7L, checkpointDir);

        backend.setCurrentKey("member-a");
        count.update(99);
        backend.setCurrentKey("member-b");
        count.update(5);
        backend.restore(handle);

        backend.setCurrentKey("member-a");
        assertThat(count.value()).contains(2);
        assertThat(terms.get()).containsExactly("one", "two");
        backend.setCurrentKey("member-b");
        assertThat(count.value()).isEmpty();
    }

    @Test
    void failsClearlyWhenAStateNameIsReusedWithAnotherShape() {
        InMemoryStateBackend backend = new InMemoryStateBackend();
        backend.valueState("session", String.class);

        assertThatThrownBy(() -> backend.listState("session", String.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("session");
    }
}
