package dev.dhruv.streaming.lms;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.dhruv.streaming.api.Collector;
import dev.dhruv.streaming.api.KeyedOperator;
import dev.dhruv.streaming.api.Operator;
import dev.dhruv.streaming.api.OperatorContext;
import dev.dhruv.streaming.api.Source;
import dev.dhruv.streaming.api.SourceContext;
import dev.dhruv.streaming.api.StreamRecord;
import dev.dhruv.streaming.api.graph.JobGraph;
import dev.dhruv.streaming.api.graph.LogicalOperator;
import dev.dhruv.streaming.api.graph.SinkNode;
import dev.dhruv.streaming.api.graph.SourceNode;
import dev.dhruv.streaming.api.graph.TransformNode;
import dev.dhruv.streaming.master.JobClient;
import dev.dhruv.streaming.metadata.EtcdMetadataStore;
import dev.dhruv.streaming.metadata.JobState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Demo 4 measures real task queues, heartbeat counters and Prometheus scrapes in three JVMs. */
@Testcontainers
class DistributedHotKeyIT {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2)).build();

    @Container
    private static final GenericContainer<?> ETCD = new GenericContainer<>(
            DockerImageName.parse("quay.io/coreos/etcd:v3.5.17"))
            .withExposedPorts(2379).withCommand("etcd", "--listen-client-urls=http://0.0.0.0:2379",
                    "--advertise-client-urls=http://0.0.0.0:2379");
    @Container
    private static final GenericContainer<?> MINIO = new GenericContainer<>(
            DockerImageName.parse("minio/minio:RELEASE.2025-04-22T22-12-26Z"))
            .withEnv("MINIO_ROOT_USER", "minioadmin").withEnv("MINIO_ROOT_PASSWORD", "minioadmin")
            .withCommand("server", "/data").withExposedPorts(9000)
            .waitingFor(Wait.forHttp("/minio/health/live").forPort(9000));

    @TempDir Path temporaryDirectory;

    @Test
    @Timeout(180)
    void measuredHotKeyBackpressureIsRelievedByTheProductionSaltedSessionPath() throws Exception {
        Path fixtureSpec = Path.of("demos/fixtures/hot-key.properties");
        if (!Files.exists(fixtureSpec)) fixtureSpec = Path.of("../demos/fixtures/hot-key.properties");
        Properties spec = new Properties();
        try (var reader = Files.newBufferedReader(fixtureSpec)) { spec.load(reader); }
        int events = Integer.parseInt(spec.getProperty("events"));
        long sourceDelay = Long.parseLong(spec.getProperty("source.record.delay.micros"));
        long workDelay = Long.parseLong(spec.getProperty("session.record.work.micros"));
        assertThat(Integer.parseInt(spec.getProperty("subtasks"))).isEqualTo(4);
        assertThat(Integer.parseInt(spec.getProperty("salt.buckets")))
                .isEqualTo(SaltedSessionAggregator.SALT_BUCKETS);
        Path artifacts = Path.of("build/demo-4").toAbsolutePath();
        Files.createDirectories(artifacts);
        Path fixture = artifacts.resolve("hot-clicks.jsonl");
        try (var writer = Files.newBufferedWriter(fixture)) {
            for (int index = 0; index < events; index++) {
                writer.write(JSON.writeValueAsString(new java.util.TreeMap<>(Map.of("memberId", spec.getProperty("member"),
                        "catalogItemId", "catalog-" + index % 37, "searchTerm", "query-" + index % 23,
                        "eventType", ClickEvent.RESULT_CLICK, "eventTimeMillis", 1_000L + index))));
                writer.newLine();
            }
        }
        int masterPort = freePort();
        int statusPort = freePort();
        String endpoint = "http://" + ETCD.getHost() + ':' + ETCD.getMappedPort(2379);
        String minio = "http://" + MINIO.getHost() + ':' + MINIO.getMappedPort(9000);
        String classpath = System.getProperty("processTestClasspath");
        assertThat(classpath).isNotBlank();
        List<ManagedProcess> processes = new ArrayList<>();
        List<Integer> metricsPorts = new ArrayList<>();
        try (EtcdMetadataStore metadata = new EtcdMetadataStore(endpoint)) {
            ManagedProcess master = start("master", "dev.dhruv.streaming.master.MasterBootstrap",
                    classpath, artifacts, Map.of("MASTER_PORT", "" + masterPort,
                            "MASTER_STATUS_PORT", "" + statusPort, "ETCD_ENDPOINTS", endpoint,
                            "JOB_CLASSPATH", classpath, "CHECKPOINT_INTERVAL_MS", "300000",
                            "RESTART_MAX_ATTEMPTS", "0"));
            processes.add(master);
            awaitPort(master, masterPort);
            for (int index = 1; index <= 3; index++) {
                int metrics = freePort();
                metricsPorts.add(metrics);
                String worker = "hot-worker-" + index;
                processes.add(start(worker, "dev.dhruv.streaming.worker.WorkerBootstrap",
                        classpath, artifacts, Map.ofEntries(
                                Map.entry("WORKER_ID", worker), Map.entry("WORKER_HOST", "127.0.0.1"),
                                Map.entry("WORKER_RPC_PORT", "" + freePort()),
                                Map.entry("WORKER_DATA_PORT", "" + freePort()),
                                Map.entry("WORKER_METRICS_PORT", "" + metrics),
                                Map.entry("WORKER_SLOTS", "12"), Map.entry("MASTER_HOST", "127.0.0.1"),
                                Map.entry("MASTER_PORT", "" + masterPort), Map.entry("ETCD_ENDPOINTS", endpoint),
                                Map.entry("JOB_CLASSPATH", classpath), Map.entry("MINIO_ENDPOINT", minio),
                                Map.entry("MINIO_ACCESS_KEY", "minioadmin"),
                                Map.entry("MINIO_SECRET_KEY", "minioadmin"),
                                Map.entry("MINIO_BUCKET", "hot-key-demo"),
                                Map.entry("CHECKPOINT_DIR", temporaryDirectory.resolve(worker).toString()))));
            }
            await("three registered worker JVMs", Duration.ofSeconds(25),
                    () -> metadata.listWorkers().size() == 3);
            Measurement unsalted = run(false, fixture, events, sourceDelay, workDelay, artifacts,
                    metadata, masterPort, statusPort, metricsPorts);
            Measurement salted = run(true, fixture, events, sourceDelay, workDelay, artifacts,
                    metadata, masterPort, statusPort, metricsPorts);
            assertThat(unsalted.counts().values().stream().filter(count -> count > 0).count()).isEqualTo(1);
            assertThat(unsalted.backpressured()).as("a real bounded hot-member channel fills").isTrue();
            assertThat(unsalted.workerBackpressured()).as("worker Prometheus observes the same real pressure").isTrue();
            assertThat(salted.counts()).hasSize(4);
            long minimum = salted.counts().values().stream().mapToLong(Long::longValue).min().orElseThrow();
            long maximum = salted.counts().values().stream().mapToLong(Long::longValue).max().orElseThrow();
            assertThat(minimum).isPositive();
            // Sixteen salts map 5/3/4/4 across the fixed four-task key-group ranges. The
            // observation must remain bounded, rather than promising perfect equal shares.
            assertThat((double) maximum / minimum).as("measured salted per-task record spread").isLessThan(1.9);
            // A scheduler pause or network flush can briefly fill one channel even after
            // salting. Compare measured sustained pressure instead of claiming zero bursts.
            assertThat(salted.pressureFraction()).as("salting relieves sustained hot-member pressure")
                    .isLessThan(unsalted.pressureFraction() / 4);
            assertThat(salted.meanQueued()).as("the average busiest session queue is smaller")
                    .isLessThan(unsalted.meanQueued() / 2);
            assertThat(salted.peakQueued()).as("the observed queue peak is smaller")
                    .isLessThan(unsalted.peakQueued());
            assertThat(salted.elapsedMillis()).as("distributed work completes sooner").isLessThan(unsalted.elapsedMillis());
            assertThat(Files.readString(salted.output())).isEqualTo(Files.readString(unsalted.output()));
            SessionRow result = JSON.readValue(Files.readString(unsalted.output()), SessionRow.class);
            assertThat(result.memberId()).isEqualTo(spec.getProperty("member"));
            assertThat(result.clickCount()).isEqualTo(events);
            assertThat(result.sessionStartMillis()).isEqualTo(1_000L);
            assertThat(result.lastEventTimeMillis()).isEqualTo(999L + events);
            assertThat(result.searchTerms()).hasSize(23);
            String report = "Fixed fixture: " + events + " clicks; same " + workDelay
                    + "us per-click session work; source delay " + sourceDelay + "us per source\n"
                    + "unsalted records/task=" + unsalted.counts() + ", peak queued=" + unsalted.peakQueued()
                    + ", pressure samples=" + unsalted.pressureSamples() + '/' + unsalted.samples()
                    + ", mean queued=" + Math.round(unsalted.meanQueued()) + ", inputDrainedMs=" + unsalted.elapsedMillis() + '\n'
                    + "salted records/task=" + salted.counts() + ", peak queued=" + salted.peakQueued()
                    + ", pressure samples=" + salted.pressureSamples() + '/' + salted.samples()
                    + ", mean queued=" + Math.round(salted.meanQueued()) + ", inputDrainedMs=" + salted.elapsedMillis() + '\n'
                    + "Session rows byte-identical. REST heartbeat samples and worker Prometheus scrapes retained.\n";
            Files.writeString(artifacts.resolve("report.txt"), report);
            System.out.println(report + "Evidence: " + artifacts);
        } finally {
            stop(processes);
        }
    }

    private Measurement run(boolean salted, Path fixture, int eventCount, long sourceDelay,
                            long workDelay, Path artifacts, EtcdMetadataStore metadata,
                            int masterPort, int statusPort, List<Integer> metricsPorts) throws Exception {
        String mode = salted ? "salted" : "unsalted";
        Path release = temporaryDirectory.resolve(mode + ".release");
        Path output = artifacts.resolve(mode + "-sessions.txt");
        JobGraph graph = productionSessionBranch(salted, fixture, release, output, sourceDelay, workDelay);
        String stage = salted ? "local-sessions" : "sessions";
        long started = System.nanoTime();
        try (JobClient client = new JobClient("127.0.0.1", masterPort)) {
            assertThat(client.submit(graph, 3, 20).getAccepted()).isTrue();
        }
        long peakQueued = 0;
        boolean backpressure = false;
        boolean workerPressureObserved = false;
        long samplesTaken = 0;
        long pressureSamples = 0;
        long totalQueued = 0;
        Map<Integer, Long> counts = new LinkedHashMap<>();
        long deadline = System.nanoTime() + Duration.ofSeconds(80).toNanos();
        try (var samples = Files.newBufferedWriter(artifacts.resolve(mode + "-tasks.jsonl"));
             var scrapes = Files.newBufferedWriter(artifacts.resolve(mode + "-workers.prom"))) {
            while (System.nanoTime() < deadline) {
                String response = get(statusPort, "/jobs/" + graph.jobId() + "/tasks");
                samples.write(response); samples.newLine(); samples.flush();
                long currentMaximumQueue = 0;
                boolean restPressure = false;
                for (JsonNode task : JSON.readTree(response)) {
                    if (stage.equals(task.path("operatorId").asText()) && task.path("heartbeatReceived").asBoolean()) {
                        counts.put(task.path("subtask").asInt(), task.path("recordsIn").asLong());
                        peakQueued = Math.max(peakQueued, task.path("inputQueuedElements").asLong());
                        currentMaximumQueue = Math.max(currentMaximumQueue, task.path("inputQueuedElements").asLong());
                        restPressure |= task.path("backpressured").asBoolean();
                    }
                }
                // Retain the actual endpoint response, including worker/operator/subtask tags.
                boolean workerBackpressure = false;
                long workerProcessed = 0;
                for (int port : metricsPorts) {
                    String scrape = get(port, "/metrics");
                    scrapes.write("# sample for " + mode + " at " + System.currentTimeMillis() + '\n');
                    scrapes.write(scrape); scrapes.flush();
                    workerBackpressure |= scrape.lines().anyMatch(line ->
                            line.startsWith("stream_engine_backpressured{")
                                    && line.contains("job=\"" + graph.jobId() + "\"")
                                    && line.contains("operator=\"" + stage + "\"")
                                    && line.endsWith(" 1.0"));
                    workerProcessed += scrape.lines().filter(line ->
                            line.startsWith("stream_engine_records_in_total{")
                                    && line.contains("job=\"" + graph.jobId() + "\"")
                                    && line.contains("operator=\"" + stage + "\""))
                            .mapToLong(line -> (long) Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1))).sum();
                }
                workerPressureObserved |= workerBackpressure;
                backpressure |= restPressure;
                samplesTaken++;
                totalQueued += currentMaximumQueue;
                if (restPressure || workerBackpressure) pressureSamples++;
                long processed = counts.values().stream().mapToLong(Long::longValue).sum();
                if (processed == eventCount) {
                    assertThat(workerProcessed).as("real worker counters agree with final heartbeat counters").isEqualTo(eventCount);
                    break;
                }
                assertThat(metadata.getJobState(graph.jobId()).orElseThrow()).isNotEqualTo(JobState.FAILED);
                Thread.sleep(250);
            }
        }
        assertThat(counts.values().stream().mapToLong(Long::longValue).sum()).isEqualTo(eventCount);
        // EOF is released only after final counters are observed, keeping completed tasks visible.
        long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        Files.createFile(release);
        await(mode + " job completion", Duration.ofSeconds(20),
                () -> metadata.getJobState(graph.jobId()).orElseThrow() == JobState.FINISHED);
        String completedTasks = get(statusPort, "/jobs/" + graph.jobId() + "/tasks");
        Files.writeString(artifacts.resolve(mode + "-finished-tasks.json"), completedTasks);
        if (salted) {
            long mergedFragments = 0;
            for (JsonNode task : JSON.readTree(completedTasks)) {
                if ("sessions".equals(task.path("operatorId").asText())) {
                    mergedFragments += task.path("recordsIn").asLong();
                }
            }
            assertThat(mergedFragments).as("the global hot-member owner receives compact fragments, not raw clicks")
                    .isEqualTo(SaltedSessionAggregator.SALT_BUCKETS);
        }
        assertThat(output).exists();
        assertThat(Files.readAllLines(output)).hasSize(1);
        return new Measurement(counts, peakQueued, backpressure, workerPressureObserved,
                samplesTaken, pressureSamples, (double) totalQueued / samplesTaken, elapsed, output);
    }

    /** Selects the real application's session DAG; changes only input/output and test work cost. */
    private static JobGraph productionSessionBranch(boolean salted, Path fixture, Path release,
                                                    Path output, long sourceDelay, long workDelay) {
        JobGraph original = LmsClickstreamJob.buildGraph(salted
                ? LmsClickstreamJob.SessionAggregationMode.SALTED : LmsClickstreamJob.SessionAggregationMode.UNSALTED);
        Set<String> ancestors = new HashSet<>();
        collectAncestors(original, "browse-sessions", ancestors);
        List<LogicalOperator> operators = new ArrayList<>();
        for (LogicalOperator operator : original.operators()) {
            if (!ancestors.contains(operator.id())) continue;
            if (operator instanceof SourceNode source) {
                operators.add(new SourceNode(source.id(), source.parallelism(),
                        new PacedFixtureSource(fixture, release, sourceDelay),
                        source.timestampAssignerOrNull(), source.outOfOrderness(), Duration.ZERO));
            } else if (operator instanceof SinkNode sink) {
                operators.add(new SinkNode(sink.id(), sink.parallelism(), sink.upstreamIds(),
                        sink.inputExchange(), new CanonicalSink(output), sink.keySelectorOrNull(), sink.partitionNameOrNull()));
            } else if (operator instanceof TransformNode transform) {
                Operator<?, ?> user = transform.operator();
                if ((!salted && transform.id().equals("sessions")) || transform.id().equals("local-sessions")) {
                    user = costly(user, workDelay);
                }
                operators.add(new TransformNode(transform.id(), transform.parallelism(), transform.upstreamIds(),
                        transform.inputExchange(), user, transform.keySelectorOrNull(), transform.partitionNameOrNull()));
            }
        }
        return new JobGraph(original.jobId(), "hot-key-" + (salted ? "salted" : "unsalted"),
                original.maxParallelism(), operators);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Operator<?, ?> costly(Operator<?, ?> operator, long delay) {
        return new CostlyKeyedOperator((KeyedOperator) operator, delay);
    }

    private static void collectAncestors(JobGraph graph, String id, Set<String> result) {
        if (result.add(id)) graph.operator(id).orElseThrow().upstreamIds()
                .forEach(upstream -> collectAncestors(graph, upstream, result));
    }

    private static final class CostlyKeyedOperator<K, I, O> implements KeyedOperator<K, I, O> {
        private final KeyedOperator<K, I, O> delegate;
        private final long delayMicros;
        CostlyKeyedOperator(KeyedOperator<K, I, O> delegate, long delayMicros) {
            this.delegate = delegate; this.delayMicros = delayMicros;
        }
        @Override public void open(OperatorContext context) throws Exception { delegate.open(context); }
        @Override public void processElement(StreamRecord<I> record, Collector<O> out) throws Exception {
            sleepMicros(delayMicros);
            delegate.processElement(record, out);
        }
        @Override public void onEventTimer(long timestamp, K key, Collector<O> out) throws Exception {
            delegate.onEventTimer(timestamp, key, out);
        }
        @Override public void onWatermark(long timestamp, Collector<O> out) throws Exception {
            delegate.onWatermark(timestamp, out);
        }
        @Override public void close() throws Exception { delegate.close(); }
    }

    private static final class PacedFixtureSource implements Source<ClickEvent> {
        private final String fixture;
        private final String release;
        private final long delayMicros;
        private transient BufferedReader reader;
        private transient ObjectMapper mapper;
        private transient int line, subtask, parallelism;
        PacedFixtureSource(Path fixture, Path release, long delayMicros) {
            this.fixture = fixture.toString(); this.release = release.toString(); this.delayMicros = delayMicros;
        }
        @Override public void open(SourceContext context) throws IOException {
            reader = Files.newBufferedReader(Path.of(fixture)); subtask = context.subtaskIndex();
            mapper = new ObjectMapper();
            parallelism = context.parallelism();
        }
        @Override public boolean poll(Collector<ClickEvent> out) throws Exception {
            String value;
            while ((value = reader.readLine()) != null) {
                if (line++ % parallelism != subtask) continue;
                sleepMicros(delayMicros);
                out.collect(mapper.readValue(value, ClickEvent.class));
                return true;
            }
            if (Files.exists(Path.of(release))) return false;
            Thread.sleep(25); return true;
        }
        @Override public void close() throws IOException { if (reader != null) reader.close(); }
    }

    private static final class CanonicalSink implements Operator<SessionRow, Void> {
        private final String output;
        private transient BufferedWriter writer;
        CanonicalSink(Path output) { this.output = output.toString(); }
        @Override public void open(OperatorContext context) throws IOException {
            writer = Files.newBufferedWriter(Path.of(output));
        }
        @Override public void processElement(StreamRecord<SessionRow> record, Collector<Void> ignored) throws IOException {
            writer.write(JSON.writeValueAsString(record.value())); writer.newLine(); writer.flush();
        }
        @Override public void close() throws IOException { if (writer != null) writer.close(); }
    }

    private ManagedProcess start(String name, String main, String classpath, Path artifacts,
                                 Map<String, String> environment) throws IOException {
        Path log = artifacts.resolve(name + ".log");
        String binary = System.getProperty("os.name").toLowerCase().contains("win") ? "java.exe" : "java";
        ProcessBuilder builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", binary).toString(),
                "-Xmx256m", "-XX:ActiveProcessorCount=2", "-cp", classpath, main);
        builder.environment().putAll(environment); builder.redirectErrorStream(true); builder.redirectOutput(log.toFile());
        return new ManagedProcess(builder.start(), log);
    }
    private static String get(int port, String path) throws Exception {
        HttpResponse<String> response = HTTP.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .timeout(Duration.ofSeconds(3)).GET().build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200); return response.body();
    }
    private static int freePort() throws IOException { try (ServerSocket socket = new ServerSocket(0)) { return socket.getLocalPort(); } }
    private static void sleepMicros(long micros) throws InterruptedException {
        // Thread.sleep requests Windows timer precision; short parkNanos delays can instead
        // round to a 15.6ms clock tick and accidentally change the fixture's service rate.
        Thread.sleep(micros / 1_000, (int) (micros % 1_000) * 1_000);
    }
    private static void awaitPort(ManagedProcess process, int port) throws Exception {
        await("master listener", Duration.ofSeconds(20), () -> {
            assertThat(process.process().isAlive()).as("master process; log " + process.log()).isTrue();
            try (Socket socket = new Socket()) { socket.connect(new InetSocketAddress("127.0.0.1", port), 200); return true; }
            catch (IOException unavailable) { return false; }
        });
    }
    private static void await(String description, Duration timeout, java.util.function.BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) { if (condition.getAsBoolean()) return; Thread.sleep(100); }
        throw new AssertionError("Timed out waiting for " + description);
    }
    private static void stop(List<ManagedProcess> processes) {
        for (ManagedProcess process : processes.reversed()) process.process().destroy();
        for (ManagedProcess process : processes.reversed()) {
            try { if (!process.process().waitFor(3, TimeUnit.SECONDS)) process.process().destroyForcibly(); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); process.process().destroyForcibly(); }
        }
    }
    private record ManagedProcess(Process process, Path log) { }
    private record Measurement(Map<Integer, Long> counts, long peakQueued, boolean backpressured,
                               boolean workerBackpressured, long samples, long pressureSamples,
                               double meanQueued, long elapsedMillis, Path output) {
        double pressureFraction() { return (double) pressureSamples / samples; }
    }
}
