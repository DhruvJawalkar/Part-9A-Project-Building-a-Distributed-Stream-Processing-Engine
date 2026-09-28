package dev.dhruv.streaming.connectors.kafka;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaSourceLagTest {

    @Test
    void offsetLagIsSortedAndNeverNegativeWhenAConsumerPositionRacesAhead() {
        TopicPartition second = new TopicPartition("clicks", 2);
        TopicPartition first = new TopicPartition("clicks", 0);

        var lag = KafkaSource.lagSnapshot(List.of(second, first),
                Map.of(first, 12L, second, 10L), Map.of(first, 7L, second, 14L));

        assertThat(lag.partitions()).extracting(partition -> partition.partition())
                .containsExactly(0, 2);
        assertThat(lag.partitions()).extracting(partition -> partition.lagRecords())
                .containsExactly(5L, 0L);
        assertThat(lag.totalLagRecords()).isEqualTo(5L);
        assertThat(lag.maxLagMillis()).isEmpty();
    }
}
