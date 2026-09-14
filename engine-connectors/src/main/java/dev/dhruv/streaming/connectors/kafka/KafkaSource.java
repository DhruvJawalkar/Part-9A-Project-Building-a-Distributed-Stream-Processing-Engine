package dev.dhruv.streaming.connectors.kafka;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.dhruv.streaming.api.Collector;
import dev.dhruv.streaming.api.Source;
import dev.dhruv.streaming.api.SourceContext;
import dev.dhruv.streaming.api.metrics.Counter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

/**
 * Reads JSON records from a Kafka topic.
 *
 * <p>The engine's one source of real input. What makes Kafka usable as a stream source -- and
 * a file or a socket not -- is that its offsets are addressable: a consumer can be told to
 * start again from a position it has already read past. Everything in Phase 4 depends on that
 * property, because recovering a job means rewinding its inputs to match the state it restored.
 *
 * <h2>Offsets in Phase 1</h2>
 *
 * <p>This version lets Kafka own the offsets: it subscribes with a consumer group and lets the
 * broker record progress automatically. That gives at-least-once delivery and nothing stronger.
 * Kill the job and restart it and processing resumes from the last committed offset, which may
 * be behind or ahead of what was actually processed, so records can be replayed or -- worse --
 * skipped.
 *
 * <p>That is deliberately the weak baseline. Phase 4 takes offsets away from Kafka entirely:
 * they become part of the checkpoint, committed only when a snapshot completes, and restored by
 * seeking rather than by asking the broker where it thinks the job got to. Reading those two
 * versions of this class against each other is the clearest way to see what exactly-once costs
 * and what it buys.
 *
 * @param <T> the record type, deserialized from JSON
 */
public final class KafkaSource<T> implements Source<T> {

    private static final long serialVersionUID = 1L;

    private static final Logger log = LoggerFactory.getLogger(KafkaSource.class);

    /** How long a single poll waits for records before returning empty. */
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(200);

    private final String topic;
    private final Class<T> valueType;
    private final String bootstrapServers;
    private final String groupId;

    // Resources, not configuration: acquired in open() on the worker that will use them, which
    // is why they are transient. Serializing this object ships the four fields above and
    // nothing else.
    private transient KafkaConsumer<String, byte[]> consumer;
    private transient ObjectMapper mapper;
    private transient Counter deserializationFailures;

    private KafkaSource(String topic, Class<T> valueType, String bootstrapServers, String groupId) {
        this.topic = Objects.requireNonNull(topic, "topic");
        this.valueType = Objects.requireNonNull(valueType, "valueType");
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
    }

    /**
     * Creates a source reading a topic into a record type.
     *
     * <p>The broker address defaults to the {@code KAFKA_BOOTSTRAP_SERVERS} environment
     * variable, or {@code localhost:9092}. The consumer group defaults to a name derived from
     * the topic.
     *
     * @param topic     the topic to read
     * @param valueType the type each message deserializes into
     * @param <T>       the record type
     * @return the source
     */
    public static <T> KafkaSource<T> of(String topic, Class<T> valueType) {
        return new KafkaSource<>(topic, valueType, defaultBootstrapServers(), "lms-" + topic);
    }

    /**
     * Returns a copy of this source reading from a different broker.
     *
     * @param bootstrapServers comma-separated broker addresses
     * @return a new source
     */
    public KafkaSource<T> withBootstrapServers(String bootstrapServers) {
        return new KafkaSource<>(topic, valueType, bootstrapServers, groupId);
    }

    /**
     * Returns a copy of this source reading under a different consumer group.
     *
     * <p>Changing the group is how a run is made to reprocess from the beginning rather than
     * resuming: a group Kafka has never seen has no committed offset to resume from.
     *
     * @param groupId the consumer group id
     * @return a new source
     */
    public KafkaSource<T> withGroupId(String groupId) {
        return new KafkaSource<>(topic, valueType, bootstrapServers, groupId);
    }

    @Override
    public void open(SourceContext context) {
        this.mapper = new ObjectMapper()
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        this.deserializationFailures = context.metrics().counter("deserialization-failures");

        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Phase 1 only. Phase 4 sets this to false and commits offsets as part of the
        // checkpoint instead, because an offset committed on Kafka's schedule has no
        // relationship to what this job has actually finished processing.
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        // Shorter than Kafka's 45-second default, because this job gets restarted constantly.
        // A consumer that dies without leaving its group keeps its partitions reserved until
        // the group coordinator gives up on it, and a fresh instance started in the meantime
        // sits through a rebalance consuming nothing. At 45 seconds that looks exactly like a
        // broken job; at 10 it looks like what it is.
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");

        this.consumer = new KafkaConsumer<>(config);

        // Subscribe rather than assign: every subtask joins the same consumer group, and Kafka
        // divides the partitions between them. Convenient, and another thing Phase 4 takes
        // back -- a job that controls its own offsets must also control its own assignment.
        consumer.subscribe(List.of(topic));

        log.info("source subtask {}/{} subscribed to '{}' at {} as group '{}'",
                context.subtaskIndex() + 1, context.parallelism(),
                topic, bootstrapServers, groupId);
    }

    @Override
    public boolean poll(Collector<T> out) {
        ConsumerRecords<String, byte[]> records = consumer.poll(POLL_TIMEOUT);
        for (ConsumerRecord<String, byte[]> record : records) {
            deserialize(record).ifPresent(out::collect);
        }
        // A Kafka topic is never exhausted. Only a bounded source ever returns false.
        return true;
    }

    private java.util.Optional<T> deserialize(ConsumerRecord<String, byte[]> record) {
        try {
            return java.util.Optional.of(mapper.readValue(record.value(), valueType));
        } catch (Exception e) {
            // A malformed message must not take the job down with it. One unparseable record
            // is a producer's problem; failing the task would make it everyone's, and the job
            // would fail again on the same record every time it restarted.
            deserializationFailures.increment();
            log.warn("dropping unparseable message at {}-{} offset {}: {}",
                    record.topic(), record.partition(), record.offset(), e.getMessage());
            return java.util.Optional.empty();
        }
    }

    @Override
    public void close() {
        if (consumer != null) {
            consumer.close();
        }
    }

    private static String defaultBootstrapServers() {
        String configured = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        return configured == null || configured.isBlank() ? "localhost:9092" : configured;
    }
}
