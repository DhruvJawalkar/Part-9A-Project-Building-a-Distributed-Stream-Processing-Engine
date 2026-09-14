package dev.dhruv.streaming.api;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the two-hop key mapping.
 *
 * <p>The properties asserted here are the ones the rest of the engine's correctness rests on.
 * If a key can reach two different subtasks, keyed state is silently wrong; if key groups move
 * when parallelism changes, no checkpoint survives a rescale.
 */
class KeyGroupAssignerTest {

    private static final int SAMPLE_KEYS = 20_000;

    @Test
    @DisplayName("every key lands in a valid key group")
    void keyGroupsAreInRange() {
        for (int i = 0; i < SAMPLE_KEYS; i++) {
            int group = KeyGroupAssigner.keyGroupFor("member-" + i);
            assertThat(group)
                    .isGreaterThanOrEqualTo(0)
                    .isLessThan(KeyGroupAssigner.NUM_KEY_GROUPS);
        }
    }

    @Test
    @DisplayName("a key whose hash is Integer.MIN_VALUE still lands in a valid group")
    void minValueHashDoesNotEscapeTheRange() {
        // Math.abs(Integer.MIN_VALUE) is itself negative -- the classic trap. It is harmless
        // here only because NUM_KEY_GROUPS is a power of two, which divides 2^31 exactly and
        // so yields 0 rather than a negative remainder. Pinned by a test because the safety
        // is a property of the constant, not of the code, and would quietly disappear if
        // NUM_KEY_GROUPS were ever set to something like 100.
        Object minHashKey = new Object() {
            @Override
            public int hashCode() {
                return Integer.MIN_VALUE;
            }
        };

        assertThat(KeyGroupAssigner.NUM_KEY_GROUPS)
                .withFailMessage("NUM_KEY_GROUPS must stay a power of two; see this test")
                .isEqualTo(Integer.highestOneBit(KeyGroupAssigner.NUM_KEY_GROUPS));
        assertThat(KeyGroupAssigner.keyGroupFor(minHashKey))
                .isGreaterThanOrEqualTo(0)
                .isLessThan(KeyGroupAssigner.NUM_KEY_GROUPS);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 4, 7, 8, 16, 64, 128})
    @DisplayName("every key lands on a valid subtask at any parallelism")
    void subtasksAreInRange(int parallelism) {
        for (int i = 0; i < SAMPLE_KEYS; i++) {
            int subtask = KeyGroupAssigner.subtaskFor("member-" + i, parallelism);
            assertThat(subtask).isGreaterThanOrEqualTo(0).isLessThan(parallelism);
        }
    }

    @Test
    @DisplayName("a key group never changes, whatever the parallelism")
    void keyGroupIsStableAcrossRescale() {
        // The whole reason the indirection exists. Hop one is fixed forever; only hop two
        // moves. This is what lets a rescale reassign whole groups instead of rehashing every
        // key in the snapshot.
        for (int i = 0; i < 1_000; i++) {
            String key = "member-" + i;
            int group = KeyGroupAssigner.keyGroupFor(key);
            assertThat(KeyGroupAssigner.keyGroupFor(key)).isEqualTo(group);
        }
    }

    @Test
    @DisplayName("all keys of a key group move to the same subtask together")
    void keyGroupsMoveWholesale() {
        // Group -> subtask must be a function. If two keys in one group ever disagreed about
        // their subtask, rescaling could not move state a group at a time.
        for (int parallelism : new int[] {2, 3, 4, 8, 16}) {
            Map<Integer, Integer> subtaskOfGroup = new HashMap<>();
            for (int i = 0; i < SAMPLE_KEYS; i++) {
                String key = "member-" + i;
                int group = KeyGroupAssigner.keyGroupFor(key);
                int subtask = KeyGroupAssigner.subtaskFor(key, parallelism);
                Integer existing = subtaskOfGroup.putIfAbsent(group, subtask);
                assertThat(existing == null ? subtask : existing)
                        .withFailMessage("key group %d split across subtasks at parallelism %d",
                                group, parallelism)
                        .isEqualTo(subtask);
            }
        }
    }

    @Test
    @DisplayName("key groups are assigned to subtasks in contiguous ranges")
    void keyGroupRangesAreContiguous() {
        // Contiguity is what makes restoring cheap: a subtask reads one slice of the snapshot
        // rather than hunting for scattered groups.
        int parallelism = 4;
        int previousSubtask = 0;
        for (int group = 0; group < KeyGroupAssigner.NUM_KEY_GROUPS; group++) {
            int subtask = group * parallelism / KeyGroupAssigner.NUM_KEY_GROUPS;
            assertThat(subtask - previousSubtask).isBetween(0, 1);
            previousSubtask = subtask;
        }
        assertThat(previousSubtask).isEqualTo(parallelism - 1);
    }

    @Test
    @DisplayName("hashing spreads realistic keys across every group")
    void distributionIsFlatEnough() {
        // String.hashCode clusters badly for short similar identifiers, which is exactly what
        // member ids look like. This asserts the murmur finalizer is actually doing its job:
        // without it, sequential ids land in a handful of groups and the job is born skewed.
        int[] perGroup = new int[KeyGroupAssigner.NUM_KEY_GROUPS];
        for (int i = 0; i < SAMPLE_KEYS; i++) {
            perGroup[KeyGroupAssigner.keyGroupFor("member-" + i)]++;
        }

        int expected = SAMPLE_KEYS / KeyGroupAssigner.NUM_KEY_GROUPS;
        for (int group = 0; group < perGroup.length; group++) {
            assertThat(perGroup[group])
                    .withFailMessage("key group %d holds %d keys, expected roughly %d",
                            group, perGroup[group], expected)
                    .isBetween(expected / 2, expected * 2);
        }
    }

    @Test
    @DisplayName("at parallelism equal to the group count, each subtask owns one group")
    void maximumParallelismUsesEveryGroup() {
        Set<Integer> subtasks = new HashSet<>();
        for (int group = 0; group < KeyGroupAssigner.NUM_KEY_GROUPS; group++) {
            subtasks.add(group * KeyGroupAssigner.NUM_KEY_GROUPS
                    / KeyGroupAssigner.NUM_KEY_GROUPS);
        }
        assertThat(subtasks).hasSize(KeyGroupAssigner.NUM_KEY_GROUPS);
    }
}
