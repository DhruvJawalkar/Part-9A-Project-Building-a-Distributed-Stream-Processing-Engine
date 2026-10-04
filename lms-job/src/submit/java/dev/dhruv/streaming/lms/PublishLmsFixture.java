package dev.dhruv.streaming.lms;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeUnit;

/** Publishes the checked-in business replay, then advances every physical input partition. */
public final class PublishLmsFixture {
    private static final String CLICKS = "lms.catalog.clicks";
    private static final String BORROWS = "lms.catalog.borrows";
    private PublishLmsFixture() { }

    /**
     * Publishes from the supplied fixture directory (default: {@code demos/fixtures}).
     * Use {@code --progress-only} as the second argument after seeding business events manually.
     */
    public static void main(String[] arguments) throws Exception {
        Path fixtures = Path.of(arguments.length == 0 ? "demos/fixtures" : arguments[0]);
        boolean progressOnly = arguments.length > 1 && "--progress-only".equals(arguments[1]);
        if (arguments.length > 2 || (arguments.length > 1 && !progressOnly)) {
            throw new IllegalArgumentException("usage: PublishLmsFixture [directory [--progress-only]]");
        }
        Properties settings = new Properties();
        settings.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
        settings.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        settings.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        settings.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        if (!progressOnly && Boolean.parseBoolean(System.getenv("FIXTURE_BOOTSTRAP_ONCE"))) {
            Properties adminSettings = new Properties();
            adminSettings.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                    settings.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
            adminSettings.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "15000");
            adminSettings.setProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "30000");
            try (AdminClient admin = AdminClient.create(adminSettings)) {
                Map<TopicPartition, OffsetSpec> requested = new LinkedHashMap<>();
                Map<TopicPartition, Long> expected = new LinkedHashMap<>();
                admin.describeTopics(List.of(CLICKS, BORROWS)).allTopicNames().get(30, TimeUnit.SECONDS)
                        .forEach((topic, description) -> description.partitions().forEach(partition ->
                                requested.put(new TopicPartition(topic, partition.partition()), OffsetSpec.latest())));
                for (String topic : List.of(CLICKS, BORROWS)) {
                    List<Integer> partitions = requested.keySet().stream()
                            .filter(partition -> partition.topic().equals(topic))
                            .map(TopicPartition::partition).sorted().toList();
                    Path business = fixtures.resolve(topic.equals(CLICKS) ? "clicks.jsonl" : "borrows.jsonl");
                    Path clocks = fixtures.resolve(topic.equals(CLICKS)
                            ? "click-watermark-progress.jsonl" : "borrow-watermark-progress.jsonl");
                    int businessCount = fixtureRows(business).size();
                    if (partitions.isEmpty() || fixtureRows(clocks).size() != partitions.size()) {
                        throw new IllegalStateException("fixture " + clocks + " must cover each partition of " + topic);
                    }
                    for (int index = 0; index < partitions.size(); index++) {
                        // Business publishing uses the same sorted round-robin partition list;
                        // each input then gets exactly one explicit clock record per partition.
                        long count = businessCount / partitions.size()
                                + (index < businessCount % partitions.size() ? 1 : 0) + 1L;
                        expected.put(new TopicPartition(topic, partitions.get(index)), count);
                    }
                }
                Map<TopicPartition, Long> offsets = new LinkedHashMap<>();
                admin.listOffsets(requested).all().get(30, TimeUnit.SECONDS)
                        .forEach((partition, position) -> offsets.put(partition, position.offset()));
                if (!shouldPublishBootstrap(offsets, expected)) {
                    System.out.println("Fixture bootstrap already present in both Kafka topics; skipping replay.");
                    return;
                }
            }
        }
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(settings)) {
            // Both business inputs precede clock records: a future clock must not evict a join
            // record whose matching business input has not yet been published.
            if (!progressOnly) {
                publish(producer, "lms.catalog.clicks", fixtures.resolve("clicks.jsonl"), false);
                publish(producer, "lms.catalog.borrows", fixtures.resolve("borrows.jsonl"), false);
            }
            publish(producer, "lms.catalog.clicks", fixtures.resolve("click-watermark-progress.jsonl"), true);
            publish(producer, "lms.catalog.borrows", fixtures.resolve("borrow-watermark-progress.jsonl"), true);
        }
    }

    /** Both inputs must start together; replaying one half after a crash would duplicate the other. */
    static boolean shouldPublishBootstrap(Map<TopicPartition, Long> current,
                                          Map<TopicPartition, Long> expected) {
        if (expected.isEmpty() || !current.keySet().equals(expected.keySet())
                || current.values().stream().anyMatch(offset -> offset < 0)
                || expected.values().stream().anyMatch(count -> count <= 0)) {
            throw new IllegalArgumentException("bootstrap needs every fixture partition and valid Kafka end offsets");
        }
        if (current.values().stream().allMatch(offset -> offset == 0)) {
            return true;
        }
        List<TopicPartition> incomplete = expected.keySet().stream()
                .filter(partition -> current.get(partition) < expected.get(partition)).toList();
        if (!incomplete.isEmpty()) {
            throw new IllegalStateException("partial or incompatible fixture bootstrap in partitions " + incomplete
                    + ": existing end offsets " + current + " are below the complete fixture baseline " + expected
                    + ". Inspect/complete the fixture manually, or reset the demo's Kafka and engine volumes "
                    + "together before seeding again; automatic replay would duplicate existing input.");
        }
        return false;
    }

    private static void publish(KafkaProducer<String, String> producer, String topic,
                                Path fixture, boolean clock) throws Exception {
        List<Integer> partitions = producer.partitionsFor(topic).stream()
                .sorted(Comparator.comparingInt(PartitionInfo::partition))
                .map(PartitionInfo::partition).toList();
        List<String> lines = fixtureRows(fixture);
        if (partitions.isEmpty() || (clock && lines.size() != partitions.size())) {
            throw new IllegalStateException("fixture " + fixture + " must cover each partition of " + topic);
        }
        for (int index = 0; index < lines.size(); index++) {
            // An explicit partition, not a producer partitioner, makes small-fixture coverage
            // independent of batching/new-batch retries. The engine's HASH exchange is unchanged.
            producer.send(new ProducerRecord<>(topic, partitions.get(index % partitions.size()),
                    null, lines.get(index))).get();
        }
        System.out.println("Published " + lines.size() + " " + (clock ? "clock" : "business")
                + " records to " + topic + " across partitions " + partitions);
    }

    private static List<String> fixtureRows(Path fixture) throws java.io.IOException {
        return Files.readAllLines(fixture).stream().filter(line -> !line.isBlank()).toList();
    }
}
