package dev.dhruv.streaming.lms;

import dev.dhruv.streaming.api.KeyGroupAssigner;
import dev.dhruv.streaming.runtime.SerializationUtil;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit-level evidence that the salt is replay-safe and breaks a single hot key into shards. */
class SaltedSessionAggregatorTest {

    @Test
    void assignsOneHotMemberAcrossEverySaltWithBoundedSkew() throws IOException {
        Properties fixture = fixture("hot-key.properties");
        String member = fixture.getProperty("member");
        int eventCount = Integer.parseInt(fixture.getProperty("events"));
        int subtasks = Integer.parseInt(fixture.getProperty("subtasks"));
        int capacity = Integer.parseInt(fixture.getProperty("per.subtask.capacity"));
        assertThat(Integer.parseInt(fixture.getProperty("salt.buckets")))
                .isEqualTo(SaltedSessionAggregator.SALT_BUCKETS);

        Map<Integer, Integer> saltCounts = new HashMap<>();
        Map<Integer, Integer> saltedSubtaskCounts = new HashMap<>();
        Map<Integer, Integer> unsaltedSubtaskCounts = new HashMap<>();
        for (int index = 0; index < eventCount; index++) {
            ClickEvent click = new ClickEvent(member, "catalog-" + (index % 37),
                    "query-" + (index % 23), ClickEvent.RESULT_CLICK, 1_000L + index);
            int salt = SaltedSessionAggregator.saltFor(click);
            saltCounts.merge(salt, 1, Integer::sum);
            SaltedSessionAggregator.SaltedMemberKey localKey =
                    new SaltedSessionAggregator.SaltedMemberKey(member, salt);
            saltedSubtaskCounts.merge(KeyGroupAssigner.subtaskFor(localKey, subtasks), 1,
                    Integer::sum);
            unsaltedSubtaskCounts.merge(KeyGroupAssigner.subtaskFor(member, subtasks), 1,
                    Integer::sum);
        }

        assertThat(saltCounts).hasSize(SaltedSessionAggregator.SALT_BUCKETS);
        assertThat(saltCounts.values()).allSatisfy(count ->
                assertThat(count).isBetween(eventCount / 20, eventCount / 12));

        assertThat(unsaltedSubtaskCounts).hasSize(1).containsValue(eventCount);
        assertThat(unsaltedSubtaskCounts.values()).allMatch(count -> count > capacity);
        assertThat(saltedSubtaskCounts).hasSize(subtasks);
        assertThat(saltedSubtaskCounts.values())
                .allMatch(count -> count <= capacity,
                        "salting should keep every local subtask below backpressure capacity");
    }

    @Test
    void saltAndWireValuesAreDeterministicAndSerializable() {
        ClickEvent click = new ClickEvent("member-hot", "catalog-9", "stream processing",
                ClickEvent.SEARCH, 123_456L);
        int salt = SaltedSessionAggregator.saltFor(click);
        SaltedSessionAggregator.SaltedClickEvent value =
                new SaltedSessionAggregator.SaltedClickEvent(click, salt);

        SaltedSessionAggregator.SaltedClickEvent restored = SerializationUtil.fromBytes(
                SerializationUtil.toBytes(value));
        assertThat(SaltedSessionAggregator.saltFor(click)).isEqualTo(salt);
        assertThat(restored).isEqualTo(value);
        assertThat(restored.key()).isEqualTo(new SaltedSessionAggregator.SaltedMemberKey(
                "member-hot", salt));
    }

    private static Properties fixture(String name) throws IOException {
        Path workingDirectory = Path.of(System.getProperty("user.dir"));
        Path path = workingDirectory.resolve(Path.of("demos", "fixtures", name));
        if (!Files.exists(path)) {
            path = workingDirectory.resolve(Path.of("..", "demos", "fixtures", name)).normalize();
        }
        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(path)) {
            properties.load(reader);
        }
        return properties;
    }
}
