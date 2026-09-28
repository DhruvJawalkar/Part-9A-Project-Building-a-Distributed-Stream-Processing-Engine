package dev.dhruv.streaming.api;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/** Optional source capability for reporting a cached, thread-safe input-lag snapshot. */
public interface SourceLagReporter {

    /**
     * Returns the latest sample, if the source has one. Callers must not use this method to
     * touch connector clients: a Kafka consumer, for example, belongs exclusively to its poll
     * thread.
     */
    Optional<SourceLag> sourceLag();

    /** Lag sample collected by a source task at one point in time. */
    record SourceLag(List<PartitionLag> partitions, OptionalLong maxLagMillis) {
        public SourceLag {
            partitions = List.copyOf(partitions);
            maxLagMillis = maxLagMillis == null ? OptionalLong.empty() : maxLagMillis;
        }

        /** Total records not yet read across the assigned partitions. */
        public long totalLagRecords() {
            return partitions.stream().mapToLong(PartitionLag::lagRecords).sum();
        }
    }

    /** One Kafka-compatible partition lag; time lag is optional when no safe timestamp exists. */
    record PartitionLag(String topic, int partition, long lagRecords) {
        public PartitionLag {
            if (topic == null || topic.isBlank() || partition < 0 || lagRecords < 0) {
                throw new IllegalArgumentException("source lag partition fields are invalid");
            }
        }
    }
}
