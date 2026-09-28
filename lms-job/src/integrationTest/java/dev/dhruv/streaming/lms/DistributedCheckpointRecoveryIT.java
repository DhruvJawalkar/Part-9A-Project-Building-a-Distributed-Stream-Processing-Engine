package dev.dhruv.streaming.lms;

import dev.dhruv.streaming.api.CheckpointableSource;
import dev.dhruv.streaming.api.Collector;
import dev.dhruv.streaming.api.KeyGroupAssigner;
import dev.dhruv.streaming.api.Operator;
import dev.dhruv.streaming.api.OperatorContext;
import dev.dhruv.streaming.api.SourceContext;
import dev.dhruv.streaming.api.StreamRecord;
import dev.dhruv.streaming.api.graph.DataStream;
import dev.dhruv.streaming.api.graph.JobGraph;
import dev.dhruv.streaming.api.state.StateHandle;
import dev.dhruv.streaming.master.JobClient;
import dev.dhruv.streaming.metadata.CompletedCheckpoint;
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
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 4's process boundary: a real master notices a killed worker and rewinds every task to
 * one portable checkpoint held in MinIO. The source is deliberately held between an open
 * session and the future event which closes it, so source cursor, RocksDB state and timers all
 * have to survive for the recovered bytes to match a failure-free run.
 */
@Testcontainers
class DistributedCheckpointRecoveryIT {

    private static final String MINIO_USER = "minioadmin";
    private static final String MINIO_PASSWORD = "minioadmin";

    @Container
    private static final GenericContainer<?> ETCD = new GenericContainer<>(
            DockerImageName.parse("quay.io/coreos/etcd:v3.5.17"))
            .withExposedPorts(2379)
            .withCommand("etcd", "--listen-client-urls=http://0.0.0.0:2379",
                    "--advertise-client-urls=http://0.0.0.0:2379");

    @Container
    private static final GenericContainer<?> MINIO = new GenericContainer<>(
            DockerImageName.parse("minio/minio:RELEASE.2025-04-22T22-12-26Z"))
            .withEnv("MINIO_ROOT_USER", MINIO_USER)
            .withEnv("MINIO_ROOT_PASSWORD", MINIO_PASSWORD)
            .withCommand("server", "/data")
            .withExposedPorts(9000)
            .waitingFor(Wait.forHttp("/minio/health/live").forPort(9000));

    @TempDir
    Path temporaryDirectory;

    @Test
    @Timeout(90)
    void killedSessionWorkerRestartsWholeJobFromPortableCheckpoint() throws Exception {
        Path fixture = writeFixture();
        byte[] baseline = runCleanBaseline(fixture);
        Path recoveredOutput = temporaryDirectory.resolve("recovered/session-rows.txt");
        Path release = temporaryDirectory.resolve("cluster.release");
        JobGraph graph = graph("phase-four-process-recovery", fixture, release, recoveredOutput);

        String etcdEndpoint = "http://" + ETCD.getHost() + ':' + ETCD.getMappedPort(2379);
        String minioEndpoint = "http://" + MINIO.getHost() + ':' + MINIO.getMappedPort(9000);
        String bucket = "phase4-recovery-" + UUID.randomUUID().toString().replace("-", "");
        String classpath = requiredProperty("processTestClasspath");
        int masterPort = freePort();
        List<ManagedProcess> processes = new ArrayList<>();
        Map<String, ManagedProcess> workers = new LinkedHashMap<>();

        try (EtcdMetadataStore metadata = new EtcdMetadataStore(etcdEndpoint)) {
            ManagedProcess master = startProcess("master", "dev.dhruv.streaming.master.MasterBootstrap",
                    classpath, Map.of(
                            "MASTER_PORT", Integer.toString(masterPort),
                            "ETCD_ENDPOINTS", etcdEndpoint,
                            "JOB_CLASSPATH", classpath,
                            // Leave enough space between attempts for the failure detector to
                            // stop checkpointing after the kill. This test wants one known cut,
                            // not a second barrier racing the intentional process death.
                            "CHECKPOINT_INTERVAL_MS", "5000",
                            "CHECKPOINT_TIMEOUT_MS", "10000",
                            "RESTART_MAX_ATTEMPTS", "3",
                            "RESTART_DELAY_MS", "250"));
            processes.add(master);
            awaitPort(master, masterPort, Duration.ofSeconds(15));

            for (int index = 1; index <= 3; index++) {
                String workerId = "worker-" + index;
                int rpcPort = freePort();
                int dataPort = freePort();
                ManagedProcess worker = startProcess(workerId,
                        "dev.dhruv.streaming.worker.WorkerBootstrap", classpath, Map.ofEntries(
                                Map.entry("WORKER_ID", workerId),
                                Map.entry("WORKER_HOST", "127.0.0.1"),
                                Map.entry("WORKER_RPC_PORT", Integer.toString(rpcPort)),
                                Map.entry("WORKER_DATA_PORT", Integer.toString(dataPort)),
                                Map.entry("WORKER_SLOTS", "8"),
                                Map.entry("MASTER_HOST", "127.0.0.1"),
                                Map.entry("MASTER_PORT", Integer.toString(masterPort)),
                                Map.entry("ETCD_ENDPOINTS", etcdEndpoint),
                                Map.entry("JOB_CLASSPATH", classpath),
                                Map.entry("CHECKPOINT_DIR", temporaryDirectory.resolve(
                                        "checkpoints/" + workerId).toString()),
                                Map.entry("MINIO_ENDPOINT", minioEndpoint),
                                Map.entry("MINIO_ACCESS_KEY", MINIO_USER),
                                Map.entry("MINIO_SECRET_KEY", MINIO_PASSWORD),
                                Map.entry("MINIO_BUCKET", bucket)));
                workers.put(workerId, worker);
                processes.add(worker);
            }

            awaitCondition("three workers to register", Duration.ofSeconds(20),
                    () -> metadata.listWorkers().size() == 3);

            try (JobClient client = new JobClient("127.0.0.1", masterPort)) {
                assertThat(client.submit(graph, 3, 15).getAccepted()).isTrue();
            }

            CompletedCheckpoint checkpoint = awaitCheckpoint(metadata, graph.jobId(),
                    Duration.ofSeconds(20));
            assertThat(checkpoint.taskStates()).hasSize(5);
            assertThat(checkpoint.taskStates().values())
                    .extracting(CompletedCheckpoint.TaskState::stateHandleUri)
                    .allMatch(uri -> uri.startsWith("minio://" + bucket + "/"));
            assertThat(recoveredOutput).exists();
            assertThat(Files.size(recoveredOutput))
                    .as("the checkpoint must precede every completed session")
                    .isZero();

            Map<String, String> initialAssignments = metadata.getAssignments(graph.jobId());
            int alphaSubtask = KeyGroupAssigner.subtaskFor("member-alpha", 3);
            String killedWorkerId = initialAssignments.get("sessions:" + alphaSubtask);
            assertThat(killedWorkerId).isNotBlank();
            ManagedProcess killed = Objects.requireNonNull(workers.get(killedWorkerId));
            killed.process().destroyForcibly();
            assertThat(killed.process().waitFor(10, TimeUnit.SECONDS))
                    .as("the selected alpha-session worker must be force-killed")
                    .isTrue();

            try {
                awaitCondition("whole-job recovery onto surviving workers", Duration.ofSeconds(35), () -> {
                    Map<String, String> recoveredAssignments = metadata.getAssignments(graph.jobId());
                    return metadata.getJobState(graph.jobId()).orElse(JobState.FAILED) == JobState.RUNNING
                            && recoveredAssignments.size() == 5
                            && !recoveredAssignments.containsValue(killedWorkerId)
                            && !recoveredAssignments.equals(initialAssignments);
                });
            } catch (AssertionError failure) {
                throw recoveryFailure(metadata, graph.jobId(), processes, failure);
            }

            Files.createFile(release);
            awaitCondition("bounded recovered job to finish", Duration.ofSeconds(20),
                    () -> metadata.getJobState(graph.jobId()).orElse(JobState.FAILED)
                            == JobState.FINISHED);

            assertThat(Files.readAllBytes(recoveredOutput)).containsExactly(baseline);
        } finally {
            stopAll(processes);
        }
    }

    private byte[] runCleanBaseline(Path fixture) throws Exception {
        Path release = temporaryDirectory.resolve("baseline.release");
        Files.createFile(release);
        Path output = temporaryDirectory.resolve("baseline/session-rows.txt");
        try (var executor = dev.dhruv.streaming.api.JobExecutors.local(
                graph("phase-four-clean-baseline", fixture, release, output))) {
            executor.start();
            executor.awaitTermination();
        }
        byte[] bytes = Files.readAllBytes(output);
        assertThat(new String(bytes, StandardCharsets.UTF_8)).isEqualTo(String.join("\n",
                "member-alpha|18000|20000|920000|2000|2|first,second",
                "member-alpha|2000000|2000000|2900000|0|1|future") + "\n");
        return bytes;
    }

    private static JobGraph graph(String name, Path fixture, Path release, Path output) {
        JobGraph.Builder job = JobGraph.named(name);
        DataStream<ClickEvent> clicks = job.source("clicks",
                        new GatedReplaySource(fixture, release, 3))
                .withEventTime(ClickEvent::eventTimeMillis, Duration.ofSeconds(5))
                .parallelism(1);
        clicks.filter("drop-bots", new BotFilter())
                .keyBy("by-member", ClickEvent::memberId)
                .process("sessions", new SessionAggregator())
                .parallelism(3)
                .sink("capture", new CanonicalFileSink(output))
                .parallelism(1);
        return job.build();
    }

    private Path writeFixture() throws IOException {
        Path fixture = temporaryDirectory.resolve("events.txt");
        Files.writeString(fixture, String.join("\n",
                "member-alpha|book-1|first|SEARCH|18000",
                "bot-crawler|book-x|ignored|SEARCH|19000",
                "member-alpha|book-1|second|RESULT_CLICK|20000",
                "member-alpha|book-2|future|SEARCH|2000000") + "\n");
        return fixture;
    }

    private ManagedProcess startProcess(String name, String mainClass, String classpath,
                                        Map<String, String> environment) throws IOException {
        Path log = temporaryDirectory.resolve("logs/" + name + ".log");
        Files.createDirectories(log.getParent());
        ProcessBuilder builder = new ProcessBuilder(javaExecutable().toString(), "-cp", classpath,
                mainClass);
        builder.environment().putAll(environment);
        builder.redirectErrorStream(true);
        builder.redirectOutput(log.toFile());
        return new ManagedProcess(name, builder.start(), log);
    }

    private static void awaitPort(ManagedProcess process, int port, Duration timeout)
            throws Exception {
        awaitCondition("master port " + port, timeout, () -> {
            requireAlive(process);
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("127.0.0.1", port), 200);
                return true;
            } catch (IOException unavailable) {
                return false;
            }
        });
    }

    private static CompletedCheckpoint awaitCheckpoint(EtcdMetadataStore metadata, String jobId,
                                                        Duration timeout) throws Exception {
        CompletedCheckpoint[] result = new CompletedCheckpoint[1];
        awaitCondition("a completed mid-window checkpoint", timeout, () -> {
            result[0] = metadata.getLatestCompletedCheckpoint(jobId).orElse(null);
            return result[0] != null;
        });
        return result[0];
    }

    private static void awaitCondition(String description, Duration timeout,
                                       BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        Throwable lastFailure = null;
        while (System.nanoTime() < deadline) {
            try {
                if (condition.getAsBoolean()) {
                    return;
                }
            } catch (RuntimeException failure) {
                lastFailure = failure;
            }
            Thread.sleep(100);
        }
        AssertionError timeoutFailure = new AssertionError("timed out waiting for " + description);
        if (lastFailure != null) {
            timeoutFailure.initCause(lastFailure);
        }
        throw timeoutFailure;
    }

    private static void requireAlive(ManagedProcess process) {
        if (!process.process().isAlive()) {
            throw new IllegalStateException(process.name() + " exited with "
                    + process.process().exitValue() + "; log: " + process.log());
        }
    }

    private static AssertionError recoveryFailure(EtcdMetadataStore metadata, String jobId,
                                                  List<ManagedProcess> processes,
                                                  AssertionError cause) {
        StringBuilder details = new StringBuilder(cause.getMessage())
                .append("\nstate=").append(metadata.getJobState(jobId).orElse(null))
                .append("\nassignments=").append(metadata.getAssignments(jobId))
                .append("\ncause=").append(metadata.getJobFailureCause(jobId).orElse("<none>"));
        for (ManagedProcess process : processes) {
            details.append("\n--- ").append(process.name()).append(" log tail ---\n");
            try {
                List<String> lines = Files.readAllLines(process.log());
                lines.stream().skip(Math.max(0, lines.size() - 80L))
                        .forEach(line -> details.append(line).append('\n'));
            } catch (IOException unreadable) {
                details.append("could not read ").append(process.log()).append(": ")
                        .append(unreadable.getMessage()).append('\n');
            }
        }
        AssertionError enriched = new AssertionError(details.toString());
        enriched.initCause(cause);
        return enriched;
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(false);
            return socket.getLocalPort();
        }
    }

    private static Path javaExecutable() {
        boolean windows = System.getProperty("os.name", "").toLowerCase().contains("win");
        return Path.of(System.getProperty("java.home"), "bin", windows ? "java.exe" : "java");
    }

    private static String requiredProperty(String name) {
        String value = System.getProperty(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("missing required system property " + name);
        }
        return value;
    }

    private static void stopAll(List<ManagedProcess> processes) {
        for (int index = processes.size() - 1; index >= 0; index--) {
            Process process = processes.get(index).process();
            if (process.isAlive()) {
                process.destroy();
            }
        }
        for (int index = processes.size() - 1; index >= 0; index--) {
            Process process = processes.get(index).process();
            try {
                if (process.isAlive() && !process.waitFor(3, TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                    process.waitFor(3, TimeUnit.SECONDS);
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                process.destroyForcibly();
            }
        }
    }

    private record ManagedProcess(String name, Process process, Path log) {
    }

    /** A bounded, checkpointable file source with a test-controlled pause after its prefix. */
    private static final class GatedReplaySource implements CheckpointableSource<ClickEvent> {
        private static final long serialVersionUID = 1L;

        private final String fixture;
        private final String release;
        private final long gatedAfterLines;
        private transient BufferedReader reader;
        private transient long lineNumber;

        private GatedReplaySource(Path fixture, Path release, long gatedAfterLines) {
            this.fixture = fixture.toString();
            this.release = release.toString();
            this.gatedAfterLines = gatedAfterLines;
        }

        @Override
        public void open(SourceContext context) throws IOException {
            reader = Files.newBufferedReader(Path.of(fixture));
            lineNumber = 0;
        }

        @Override
        public boolean poll(Collector<ClickEvent> out) throws Exception {
            requireOpen();
            if (lineNumber >= gatedAfterLines && !Files.exists(Path.of(release))) {
                Thread.sleep(25);
                return true;
            }
            String line = reader.readLine();
            if (line == null) {
                return false;
            }
            lineNumber++;
            String[] fields = line.split("\\|", -1);
            if (fields.length != 5) {
                throw new IOException("invalid replay line " + lineNumber + ": " + line);
            }
            out.collect(new ClickEvent(fields[0], fields[1], fields[2], fields[3],
                    Long.parseLong(fields[4])));
            return true;
        }

        @Override
        public StateHandle snapshot(long checkpointId, Path checkpointDir) throws IOException {
            requireOpen();
            Files.createDirectories(checkpointDir);
            Path stateFile = checkpointDir.resolve("gated-replay-" + checkpointId + ".properties");
            Properties state = new Properties();
            state.setProperty("fixture", fixture);
            state.setProperty("line-number", Long.toString(lineNumber));
            try (BufferedWriter writer = Files.newBufferedWriter(stateFile)) {
                state.store(writer, "gated replay cursor");
            }
            return new StateHandle(stateFile.toAbsolutePath().toUri(), Files.size(stateFile));
        }

        @Override
        public void restore(StateHandle handle) throws IOException {
            requireOpen();
            Properties state = new Properties();
            try (BufferedReader stateReader = Files.newBufferedReader(pathFor(handle.uri()))) {
                state.load(stateReader);
            }
            if (!fixture.equals(state.getProperty("fixture"))) {
                throw new IOException("checkpoint belongs to another replay fixture");
            }
            long cursor = Long.parseLong(state.getProperty("line-number"));
            reader.close();
            reader = Files.newBufferedReader(Path.of(fixture));
            lineNumber = 0;
            while (lineNumber < cursor) {
                if (reader.readLine() == null) {
                    throw new IOException("checkpoint cursor is past end of replay fixture");
                }
                lineNumber++;
            }
        }

        @Override
        public void close() throws IOException {
            if (reader != null) {
                reader.close();
                reader = null;
            }
        }

        private void requireOpen() {
            if (reader == null) {
                throw new IllegalStateException("source is not open");
            }
        }

        private static Path pathFor(URI uri) throws IOException {
            if (!"file".equalsIgnoreCase(uri.getScheme())) {
                throw new IOException("source restore requires a materialized file handle: " + uri);
            }
            return Path.of(uri);
        }
    }

    /** One deterministic file sink; its file is empty at the mid-window recovery cut. */
    private static final class CanonicalFileSink implements Operator<SessionRow, Void> {
        private static final long serialVersionUID = 1L;

        private final String outputFile;
        private transient OutputStream output;

        private CanonicalFileSink(Path outputFile) {
            this.outputFile = outputFile.toString();
        }

        @Override
        public void open(OperatorContext context) throws IOException {
            Path path = Path.of(outputFile);
            Files.createDirectories(path.toAbsolutePath().getParent());
            output = Files.newOutputStream(path, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        }

        @Override
        public void processElement(StreamRecord<SessionRow> record, Collector<Void> ignored)
                throws IOException {
            SessionRow row = record.value();
            String encoded = String.join("|", row.memberId(),
                    Long.toString(row.sessionStartMillis()),
                    Long.toString(row.lastEventTimeMillis()),
                    Long.toString(row.sessionEndMillis()),
                    Long.toString(row.durationMillis()),
                    Long.toString(row.clickCount()),
                    String.join(",", row.searchTerms())) + "\n";
            output.write(encoded.getBytes(StandardCharsets.UTF_8));
            output.flush();
        }

        @Override
        public void close() throws IOException {
            if (output != null) {
                output.close();
                output = null;
            }
        }
    }
}
