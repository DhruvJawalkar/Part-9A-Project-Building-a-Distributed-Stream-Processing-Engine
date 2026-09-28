package dev.dhruv.streaming.connectors.kafka;

import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the source-side parts of recovery without requiring a broker. */
class KafkaSourceCheckpointTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void assignsPartitionsDeterministicallyWithoutAConsumerGroup() {
        List<PartitionInfo> partitions = List.of(
                new PartitionInfo("clicks", 0, null, null, null),
                new PartitionInfo("clicks", 1, null, null, null),
                new PartitionInfo("clicks", 2, null, null, null),
                new PartitionInfo("clicks", 3, null, null, null),
                new PartitionInfo("clicks", 4, null, null, null),
                new PartitionInfo("clicks", 5, null, null, null));

        assertThat(KafkaSource.ownedPartitions("clicks", partitions, 0, 3))
                .containsExactly(new TopicPartition("clicks", 0), new TopicPartition("clicks", 3));
        assertThat(KafkaSource.ownedPartitions("clicks", partitions, 1, 3))
                .containsExactly(new TopicPartition("clicks", 1), new TopicPartition("clicks", 4));
        assertThat(KafkaSource.ownedPartitions("clicks", partitions, 2, 3))
                .containsExactly(new TopicPartition("clicks", 2), new TopicPartition("clicks", 5));
    }

    @Test
    void snapshotsAndRestoresNextOffsetsRatherThanBrokerCommits() throws Exception {
        Path checkpoint = temporaryDirectory.resolve("offsets.properties");
        Map<TopicPartition, Long> expected = Map.of(
                new TopicPartition("clicks", 0), 15L,
                new TopicPartition("clicks", 2), 42L);

        KafkaSource.writeOffsetSnapshot(checkpoint, "clicks", expected);

        assertThat(KafkaSource.readOffsetSnapshot(checkpoint, "clicks")).isEqualTo(expected);
    }

    @Test
    void rejectsAnOffsetHandleForAnotherTopic() throws Exception {
        Path checkpoint = temporaryDirectory.resolve("offsets.properties");
        KafkaSource.writeOffsetSnapshot(checkpoint, "clicks", Map.of(new TopicPartition("clicks", 0), 1L));

        assertThatThrownBy(() -> KafkaSource.readOffsetSnapshot(checkpoint, "borrows"))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("belongs to topic 'clicks'");
    }
}
