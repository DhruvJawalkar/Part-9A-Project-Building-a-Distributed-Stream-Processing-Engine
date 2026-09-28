package dev.dhruv.streaming.connectors.kafka;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.dhruv.streaming.api.CheckpointableSource;
import dev.dhruv.streaming.api.SourceLagReporter;
import dev.dhruv.streaming.api.Collector;
import dev.dhruv.streaming.api.SourceContext;
import dev.dhruv.streaming.api.metrics.Counter;
import dev.dhruv.streaming.api.state.StateHandle;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;

/**
 * Reads JSON records from Kafka using checkpoint-owned offsets.
 *
 * <p>Each source subtask owns partitions whose index is congruent to its subtask index modulo
 * source parallelism. A checkpoint stores the <em>next</em> offset for each owned partition,
 * and recovery seeks to it before polling. Kafka consumer-group offsets are never read or
 * written: their independent commit schedule cannot be consistent with operator state.
 *
 * @param <T> the record type, deserialized from JSON
 */
public final class KafkaSource<T> implements CheckpointableSource<T>, SourceLagReporter {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(KafkaSource.class);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(200);

    private final String topic;
    private final Class<T> valueType;
    private final String bootstrapServers;

    private transient KafkaConsumer<String, byte[]> consumer;
    private transient ObjectMapper mapper;
    private transient Counter deserializationFailures;
    private transient List<TopicPartition> assignedPartitions;
    private transient Map<TopicPartition, Long> nextOffsets;
    private transient Map<TopicPartition, Long> restoredOffsets;
    private transient volatile Optional<SourceLag> latestLag = Optional.empty();
    private transient long lastLagSampleNanos;

    private KafkaSource(String topic, Class<T> valueType, String bootstrapServers) {
        this.topic = Objects.requireNonNull(topic, "topic");
        this.valueType = Objects.requireNonNull(valueType, "valueType");
        this.bootstrapServers = Objects.requireNonNull(bootstrapServers, "bootstrapServers");
    }

    /**
     * Creates a source reading a topic into a record type.
     *
     * <p>The broker address defaults to {@code KAFKA_BOOTSTRAP_SERVERS}, or
     * {@code localhost:9092}. A source is configuration only until {@link #open(SourceContext)}
     * runs on its assigned worker, so this factory remains safe to serialize in a job graph.
     *
     * @param topic Kafka topic to read
     * @param valueType JSON value type
     * @param <T> value type
     * @return a source configured for the topic
     */
    public static <T> KafkaSource<T> of(String topic, Class<T> valueType) {
        return new KafkaSource<>(topic, valueType, defaultBootstrapServers());
    }

    /**
     * Returns a copy of this source reading from a different broker.
     *
     * @param bootstrapServers comma-separated broker addresses
     * @return an independently configured source
     */
    public KafkaSource<T> withBootstrapServers(String bootstrapServers) {
        return new KafkaSource<>(topic, valueType, bootstrapServers);
    }

    /**
     * Retained for source compatibility with the early phase API.
     *
     * <p>Phase 4 intentionally has no Kafka group. Its positions live in engine checkpoints,
     * so the requested group id is ignored rather than becoming an accidental commit channel.
     */
    @Deprecated(forRemoval = false)
    public KafkaSource<T> withGroupId(String ignoredGroupId) {
        Objects.requireNonNull(ignoredGroupId, "groupId");
        return this;
    }

    @Override
    public void open(SourceContext context) throws IOException {
        mapper = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        deserializationFailures = context.metrics().counter("deserialization-failures");

        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumer = new KafkaConsumer<>(config);

        assignedPartitions = ownedPartitions(topic, consumer.partitionsFor(topic),
                context.subtaskIndex(), context.parallelism());
        consumer.assign(assignedPartitions);
        nextOffsets = new LinkedHashMap<>();
        if (restoredOffsets == null || restoredOffsets.isEmpty()) {
            consumer.seekToBeginning(assignedPartitions);
        } else {
            applyRestoredOffsets();
        }
        for (TopicPartition partition : assignedPartitions) {
            nextOffsets.putIfAbsent(partition, consumer.position(partition));
        }

        log.info("source subtask {}/{} assigned {} partitions of '{}' at {} (checkpoint-owned offsets)",
                context.subtaskIndex() + 1, context.parallelism(), assignedPartitions.size(), topic,
                bootstrapServers);
    }

    @Override
    public boolean poll(Collector<T> out) {
        ConsumerRecords<String, byte[]> records = consumer.poll(POLL_TIMEOUT);
        for (ConsumerRecord<String, byte[]> record : records) {
            deserialize(record).ifPresent(out::collect);
            // A malformed record is still consumed: it was counted and dropped, so replaying it
            // forever after recovery would pin the partition.
            nextOffsets.put(new TopicPartition(record.topic(), record.partition()), record.offset() + 1);
        }
        sampleLagIfDue();
        return true;
    }

    /**
     * KafkaConsumer is confined to the source task thread, so this samples here rather than in a
     * metrics scrape. Offset lag is exact at the instant endOffsets returns; Kafka cannot supply
     * a safe age for an unread end offset without reading the record, so maxLagMillis stays absent.
     */
    private void sampleLagIfDue() {
        long now = System.nanoTime();
        if (now - lastLagSampleNanos < Duration.ofSeconds(1).toNanos()) {
            return;
        }
        try {
            Map<TopicPartition, Long> ends = consumer.endOffsets(assignedPartitions);
            latestLag = Optional.of(lagSnapshot(assignedPartitions, ends, nextOffsets));
            lastLagSampleNanos = now;
        } catch (RuntimeException failure) {
            // A lag sample must not make a healthy data plane fail. Preserve the last truthful
            // sample and try again on the next cadence.
            log.debug("could not sample Kafka lag for topic {}", topic, failure);
            lastLagSampleNanos = now;
        }
    }

    @Override
    public Optional<SourceLag> sourceLag() {
        return latestLag == null ? Optional.empty() : latestLag;
    }

    /** Pure conversion kept separate so lag arithmetic is testable without a Kafka broker. */
    static SourceLag lagSnapshot(List<TopicPartition> assigned,
                                 Map<TopicPartition, Long> ends,
                                 Map<TopicPartition, Long> next) {
        List<PartitionLag> partitions = assigned.stream().sorted(Comparator
                        .comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition))
                .map(partition -> new PartitionLag(partition.topic(), partition.partition(),
                        Math.max(0, ends.getOrDefault(partition, 0L)
                                - next.getOrDefault(partition, 0L))))
                .toList();
        return new SourceLag(partitions, OptionalLong.empty());
    }

    @Override
    public StateHandle snapshot(long checkpointId, Path checkpointDir) throws IOException {
        if (nextOffsets == null) {
            throw new IOException("Kafka source has not been opened; no offsets can be snapshotted");
        }
        Files.createDirectories(checkpointDir);
        Path snapshot = checkpointDir.resolve("kafka-offsets-" + checkpointId + ".properties");
        writeOffsetSnapshot(snapshot, topic, nextOffsets);
        return new StateHandle(snapshot.toAbsolutePath().toUri(), Files.size(snapshot));
    }

    @Override
    public void restore(StateHandle handle) throws IOException {
        Objects.requireNonNull(handle, "handle");
        restoredOffsets = readOffsetSnapshot(pathFor(handle.uri()), topic);
        if (consumer != null && assignedPartitions != null) {
            applyRestoredOffsets();
        }
    }

    private Optional<T> deserialize(ConsumerRecord<String, byte[]> record) {
        try {
            return Optional.of(mapper.readValue(record.value(), valueType));
        } catch (Exception e) {
            deserializationFailures.increment();
            log.warn("dropping unparseable message at {}-{} offset {}: {}",
                    record.topic(), record.partition(), record.offset(), e.getMessage());
            return Optional.empty();
        }
    }

    @Override
    public void close() {
        if (consumer != null) {
            consumer.close();
        }
    }

    static List<TopicPartition> ownedPartitions(String topic,
                                                List<PartitionInfo> partitions,
                                                int subtaskIndex,
                                                int parallelism) {
        if (parallelism < 1 || subtaskIndex < 0 || subtaskIndex >= parallelism) {
            throw new IllegalArgumentException("subtask index must be in [0, parallelism)");
        }
        return partitions.stream()
                .map(PartitionInfo::partition)
                .sorted()
                .filter(partition -> partition % parallelism == subtaskIndex)
                .map(partition -> new TopicPartition(topic, partition))
                .toList();
    }

    static void writeOffsetSnapshot(Path snapshot, String topic, Map<TopicPartition, Long> offsets)
            throws IOException {
        Properties state = new Properties();
        state.setProperty("topic", topic);
        offsets.entrySet().stream()
                .sorted(Map.Entry.comparingByKey(Comparator.comparingInt(TopicPartition::partition)))
                .forEach(entry -> state.setProperty("partition." + entry.getKey().partition(),
                        Long.toString(entry.getValue())));
        try (Writer writer = Files.newBufferedWriter(snapshot)) {
            state.store(writer, "Kafka next offsets; written before checkpoint barrier");
        }
    }

    static Map<TopicPartition, Long> readOffsetSnapshot(Path snapshot, String expectedTopic)
            throws IOException {
        Properties state = new Properties();
        try (Reader reader = Files.newBufferedReader(snapshot)) {
            state.load(reader);
        }
        String savedTopic = state.getProperty("topic");
        if (!expectedTopic.equals(savedTopic)) {
            throw new IOException("Kafka checkpoint belongs to topic '" + savedTopic
                    + "', not '" + expectedTopic + "'");
        }
        Map<TopicPartition, Long> offsets = new LinkedHashMap<>();
        for (String name : state.stringPropertyNames()) {
            if (!name.startsWith("partition.")) {
                continue;
            }
            try {
                int partition = Integer.parseInt(name.substring("partition.".length()));
                long offset = Long.parseLong(state.getProperty(name));
                if (partition < 0 || offset < 0) {
                    throw new NumberFormatException("partition and offset must be non-negative");
                }
                offsets.put(new TopicPartition(expectedTopic, partition), offset);
            } catch (NumberFormatException e) {
                throw new IOException("invalid Kafka checkpoint entry '" + name + "'", e);
            }
        }
        return offsets;
    }

    private void applyRestoredOffsets() throws IOException {
        for (TopicPartition partition : assignedPartitions) {
            Long offset = restoredOffsets.get(partition);
            if (offset == null) {
                throw new IOException("checkpoint does not contain an offset for assigned partition "
                        + partition);
            }
            consumer.seek(partition, offset);
            nextOffsets.put(partition, offset);
        }
    }

    private static Path pathFor(URI uri) throws IOException {
        if (!"file".equalsIgnoreCase(uri.getScheme())) {
            throw new IOException("Kafka source can only restore a file checkpoint handle: " + uri);
        }
        try {
            return Path.of(uri);
        } catch (IllegalArgumentException e) {
            throw new IOException("invalid Kafka checkpoint handle: " + uri, e);
        }
    }

    private static String defaultBootstrapServers() {
        String configured = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        return configured == null || configured.isBlank() ? "localhost:9092" : configured;
    }
}
