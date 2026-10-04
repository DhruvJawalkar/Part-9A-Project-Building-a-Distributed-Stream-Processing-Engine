package dev.dhruv.streaming.lms;

import dev.dhruv.streaming.api.CheckpointableSource;
import dev.dhruv.streaming.api.CheckpointListener;
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
import dev.dhruv.streaming.connectors.iceberg.IcebergSink;
import io.minio.MinioClient;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.StandardCopyOption;
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
    private static final Network NETWORK = Network.newNetwork();

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
            .withNetwork(NETWORK)
            .withNetworkAliases("recovery-minio")
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
        boolean passed = false;

        try (EtcdMetadataStore metadata = new EtcdMetadataStore(etcdEndpoint)) {
            ManagedProcess master = startProcess("master", "dev.dhruv.streaming.master.MasterBootstrap",
                    classpath, Map.of(
                            "MASTER_PORT", Integer.toString(masterPort),
                            // The status endpoint is a second listener. Give every acceptance
                            // process an ephemeral port so this test can coexist with Compose or
                            // another build on the developer's machine.
                            "MASTER_STATUS_PORT", Integer.toString(freePort()),
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
                                Map.entry("WORKER_METRICS_PORT", Integer.toString(freePort())),
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
            // member-beta maps to session subtask 2 at this parallelism. That task does not
            // share worker placement with the singleton sink in this deterministic plan, so
            // the intentional kill cannot race the just-completed sink notification.
            int sessionSubtask = KeyGroupAssigner.subtaskFor("member-beta", 3);
            String killedWorkerId = initialAssignments.get("sessions:" + sessionSubtask);
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
            passed = true;
        } finally {
            stopAll(processes);
            Path evidence = exportEvidence("demo-1-session-worker-loss", fixture, processes, passed);
            if (Files.exists(recoveredOutput)) {
                Files.copy(recoveredOutput, evidence.resolve("session-rows.txt"),
                        StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }

    @Test
    @Timeout(100)
    void killedMasterRestoresWholeJobWhileWorkersKeepProcessing() throws Exception {
        Path fixture = writeFixture();
        byte[] baseline = runCleanBaseline(fixture);
        Path output = temporaryDirectory.resolve("master-recovered/session-rows.txt");
        Path release = temporaryDirectory.resolve("master-final.release");
        Path outageRelease = temporaryDirectory.resolve("master-outage.release");
        Path outageProgress = temporaryDirectory.resolve("master-outage.progress");
        JobGraph graph = graph("phase-seven-master-recovery",
                new GatedReplaySource(fixture, release, 3, outageRelease, outageProgress), output);

        String etcdEndpoint = "http://" + ETCD.getHost() + ':' + ETCD.getMappedPort(2379);
        String minioEndpoint = "http://" + MINIO.getHost() + ':' + MINIO.getMappedPort(9000);
        String bucket = "master-recovery-" + UUID.randomUUID().toString().replace("-", "");
        String classpath = requiredProperty("processTestClasspath");
        int masterPort = freePort();
        int statusPort = freePort();
        Map<String, String> masterEnvironment = Map.of(
                "MASTER_PORT", Integer.toString(masterPort),
                "MASTER_STATUS_PORT", Integer.toString(statusPort),
                "ETCD_ENDPOINTS", etcdEndpoint,
                "JOB_CLASSPATH", classpath,
                "CHECKPOINT_INTERVAL_MS", "5000",
                "CHECKPOINT_TIMEOUT_MS", "10000",
                "RESTART_MAX_ATTEMPTS", "3",
                "RESTART_DELAY_MS", "250");
        List<ManagedProcess> processes = new ArrayList<>();
        List<ManagedProcess> workers = new ArrayList<>();
        boolean passed = false;

        try (EtcdMetadataStore metadata = new EtcdMetadataStore(etcdEndpoint)) {
            ManagedProcess master = startProcess("original-master",
                    "dev.dhruv.streaming.master.MasterBootstrap", classpath, masterEnvironment);
            processes.add(master);
            awaitPort(master, masterPort, Duration.ofSeconds(15));
            for (int index = 1; index <= 3; index++) {
                String workerId = "master-recovery-worker-" + index;
                ManagedProcess worker = startProcess(workerId,
                        "dev.dhruv.streaming.worker.WorkerBootstrap", classpath, Map.ofEntries(
                                Map.entry("WORKER_ID", workerId),
                                Map.entry("WORKER_HOST", "127.0.0.1"),
                                Map.entry("WORKER_RPC_PORT", Integer.toString(freePort())),
                                Map.entry("WORKER_DATA_PORT", Integer.toString(freePort())),
                                Map.entry("WORKER_METRICS_PORT", Integer.toString(freePort())),
                                Map.entry("WORKER_SLOTS", "8"),
                                Map.entry("MASTER_HOST", "127.0.0.1"),
                                Map.entry("MASTER_PORT", Integer.toString(masterPort)),
                                Map.entry("ETCD_ENDPOINTS", etcdEndpoint),
                                Map.entry("JOB_CLASSPATH", classpath),
                                Map.entry("CHECKPOINT_DIR", temporaryDirectory.resolve(
                                        "master-checkpoints/" + workerId).toString()),
                                Map.entry("MINIO_ENDPOINT", minioEndpoint),
                                Map.entry("MINIO_ACCESS_KEY", MINIO_USER),
                                Map.entry("MINIO_SECRET_KEY", MINIO_PASSWORD),
                                Map.entry("MINIO_BUCKET", bucket)));
                workers.add(worker);
                processes.add(worker);
            }
            awaitCondition("three master-recovery workers to register", Duration.ofSeconds(20),
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
            assertThat(Files.size(output)).isZero();

            master.process().destroyForcibly();
            assertThat(master.process().waitFor(10, TimeUnit.SECONDS)).isTrue();
            assertThat(workers).allSatisfy(worker -> assertThat(worker.process().isAlive()).isTrue());
            Files.createFile(outageRelease);
            awaitCondition("a worker to process a click with the master down", Duration.ofSeconds(10),
                    () -> Files.exists(outageProgress));
            assertThat(Files.readString(outageProgress)).isEqualTo("4");
            assertThat(master.process().isAlive()).isFalse();
            assertThat(Files.size(output)).as("the session remains open during the outage").isZero();
            System.out.println("MASTER DOWN: worker replay advanced from checkpoint cursor 3 to 4");

            ManagedProcess restarted = startProcess("restarted-master",
                    "dev.dhruv.streaming.master.MasterBootstrap", classpath, masterEnvironment);
            processes.add(restarted);
            awaitPort(restarted, masterPort, Duration.ofSeconds(15));
            // RUNNING alone can be stale etcd state from before the crash. The new master's
            // completion log plus a new all-task checkpoint prove that recovery actually ran.
            awaitCondition("fresh master to restore every task", Duration.ofSeconds(30), () -> {
                requireAlive(restarted);
                try {
                    return Files.readString(restarted.log()).contains("job " + graph.jobId()
                            + " restarted from checkpoint " + checkpoint.checkpointId())
                            && metadata.getJobState(graph.jobId()).orElse(JobState.FAILED)
                            == JobState.RUNNING;
                } catch (IOException unreadable) {
                    throw new IllegalStateException(unreadable);
                }
            });
            awaitCondition("a fresh checkpoint from all restored tasks", Duration.ofSeconds(20),
                    () -> metadata.getLatestCompletedCheckpoint(graph.jobId())
                            .filter(next -> next.checkpointId() > checkpoint.checkpointId()
                                    && next.taskStates().size() == 5).isPresent());
            Files.createFile(release);
            awaitCondition("master-recovered job to finish", Duration.ofSeconds(20),
                    () -> metadata.getJobState(graph.jobId()).orElse(JobState.FAILED)
                            == JobState.FINISHED);
            assertThat(Files.readAllBytes(output)).containsExactly(baseline);
            System.out.println("MASTER RECOVERED: all tasks restored; rows equal the clean replay");
            passed = true;
        } finally {
            stopAll(processes);
            Path evidence = exportEvidence("demo-2-master-loss", fixture, processes, passed);
            if (Files.exists(output)) {
                Files.copy(output, evidence.resolve("session-rows.txt"), StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }

    @Test
    @Timeout(150)
    void killedSinkWorkerLeavesOrphanAndRecoversExactlyOnceIcebergRows() throws Exception {
        Path fixture = writeFixture();
        List<String> expected = new String(runCleanBaseline(fixture), StandardCharsets.UTF_8)
                .lines().sorted().toList();
        String minioEndpoint = "http://" + MINIO.getHost() + ':' + MINIO.getMappedPort(9000);
        MinioClient objects = MinioClient.builder().endpoint(minioEndpoint)
                .credentials(MINIO_USER, MINIO_PASSWORD).build();
        if (!objects.bucketExists(BucketExistsArgs.builder().bucket("warehouse").build())) {
            objects.makeBucket(MakeBucketArgs.builder().bucket("warehouse").build());
        }
        try (GenericContainer<?> rest = new GenericContainer<>(
                DockerImageName.parse("apache/iceberg-rest-fixture:1.10.1"))
                .withNetwork(NETWORK)
                .withExposedPorts(8181)
                .withEnv("CATALOG_URI", "jdbc:sqlite:/tmp/recovery-catalog.db")
                .withEnv("CATALOG_WAREHOUSE", "s3://warehouse/")
                .withEnv("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
                .withEnv("CATALOG_S3_ENDPOINT", "http://recovery-minio:9000")
                .withEnv("CATALOG_S3_PATH__STYLE__ACCESS", "true")
                .withEnv("AWS_ACCESS_KEY_ID", MINIO_USER)
                .withEnv("AWS_SECRET_ACCESS_KEY", MINIO_PASSWORD)
                .withEnv("AWS_REGION", "us-east-1")
                .waitingFor(Wait.forHttp("/v1/config").forPort(8181))) {
            rest.start();
            String catalogUri = "http://" + rest.getHost() + ':' + rest.getMappedPort(8181);
            Map<String, String> properties = Map.of(
                    "uri", catalogUri,
                    "warehouse", "s3://warehouse/",
                    "io-impl", "org.apache.iceberg.aws.s3.S3FileIO",
                    "s3.endpoint", minioEndpoint,
                    "s3.path-style-access", "true",
                    "s3.access-key-id", MINIO_USER,
                    "s3.secret-access-key", MINIO_PASSWORD,
                    "client.region", "us-east-1");
            try (RESTCatalog catalog = new RESTCatalog()) {
                catalog.initialize("recovery", properties);
                Namespace namespace = Namespace.of("process_recovery_"
                        + UUID.randomUUID().toString().replace("-", ""));
                catalog.createNamespace(namespace);
                Schema schema = new Schema(Types.NestedField.required(1,
                        "canonical_row", Types.StringType.get()));
                TableIdentifier cleanId = TableIdentifier.of(namespace, "clean_sessions");
                TableIdentifier recoveredId = TableIdentifier.of(namespace, "recovered_sessions");
                Table cleanTable = catalog.createTable(cleanId, schema, PartitionSpec.unpartitioned());
                Table recoveredTable = catalog.createTable(recoveredId, schema, PartitionSpec.unpartitioned());
                runIcebergRecovery(fixture, catalogUri, properties, cleanId, cleanTable,
                        recoveredId, recoveredTable, expected);
            }
        }
    }

    private void runIcebergRecovery(Path fixture, String catalogUri, Map<String, String> properties,
                                    TableIdentifier cleanId, Table cleanTable,
                                    TableIdentifier recoveredId, Table recoveredTable,
                                    List<String> expected) throws Exception {
        String etcdEndpoint = "http://" + ETCD.getHost() + ':' + ETCD.getMappedPort(2379);
        String minioEndpoint = "http://" + MINIO.getHost() + ':' + MINIO.getMappedPort(9000);
        String bucket = "iceberg-recovery-" + UUID.randomUUID().toString().replace("-", "");
        String classpath = requiredProperty("processTestClasspath");
        int masterPort = freePort();
        List<ManagedProcess> processes = new ArrayList<>();
        boolean passed = false;
        try (EtcdMetadataStore metadata = new EtcdMetadataStore(etcdEndpoint)) {
            Map<String, ManagedProcess> workers = startRecoveryCluster(processes, metadata,
                    classpath, masterPort, etcdEndpoint, minioEndpoint, bucket);
            Path cleanRelease = temporaryDirectory.resolve("iceberg-clean.release");
            Path cleanEof = temporaryDirectory.resolve("iceberg-clean-eof.release");
            Files.createFile(cleanRelease);
            JobGraph clean = graph("iceberg-clean-replay",
                    new GatedReplaySource(fixture, cleanRelease, 3, null, null, cleanEof, 6),
                    icebergSink(catalogUri, properties, cleanId, null), "clean-");
            submit(clean, masterPort);
            awaitCondition("clean Iceberg rows to be atomically committed", Duration.ofSeconds(25),
                    () -> readIceberg(cleanTable).rows().equals(expected));
            Files.createFile(cleanEof);
            awaitCondition("clean transactional job to finish", Duration.ofSeconds(10),
                    () -> metadata.getJobState(clean.jobId()).orElse(JobState.FAILED) == JobState.FINISHED);
            assertThat(readIceberg(cleanTable).rows()).containsExactlyElementsOf(expected);

            Path recoveredRelease = temporaryDirectory.resolve("iceberg-replay.release");
            Path recoveredEof = temporaryDirectory.resolve("iceberg-replay-eof.release");
            Path orphanMarker = temporaryDirectory.resolve("iceberg-orphan.paths");
            JobGraph recovered = graph("iceberg-worker-recovery",
                    new GatedReplaySource(fixture, recoveredRelease, 3, null, null, recoveredEof, 6),
                    icebergSink(catalogUri, properties, recoveredId, orphanMarker));
            submit(recovered, masterPort);
            CompletedCheckpoint checkpoint = awaitCheckpoint(metadata, recovered.jobId(),
                    Duration.ofSeconds(20));
            assertThat(readIceberg(recoveredTable).rows()).isEmpty();
            Files.createFile(recoveredRelease);
            awaitCondition("sink to close uncommitted Parquet before its checkpoint ACK",
                    Duration.ofSeconds(20), () -> Files.exists(orphanMarker));
            List<String> orphans = Files.readAllLines(orphanMarker);
            assertThat(orphans).isNotEmpty();
            for (String path : orphans) {
                assertThat(recoveredTable.io().newInputFile(path).exists()).isTrue();
            }
            assertThat(readIceberg(recoveredTable).dataPaths()).doesNotContainAnyElementsOf(orphans);
            assertThat(readIceberg(recoveredTable).rows()).isEmpty();
            assertThat(metadata.getLatestCompletedCheckpoint(recovered.jobId()).orElseThrow()
                    .checkpointId()).isEqualTo(checkpoint.checkpointId());

            String killedWorkerId = metadata.getAssignments(recovered.jobId()).get("capture:0");
            ManagedProcess killed = Objects.requireNonNull(workers.get(killedWorkerId));
            killed.process().destroyForcibly();
            assertThat(killed.process().waitFor(10, TimeUnit.SECONDS)).isTrue();
            awaitCondition("transactional whole-job recovery onto surviving workers",
                    Duration.ofSeconds(35), () -> metadata.getJobState(recovered.jobId())
                            .orElse(JobState.FAILED) == JobState.RUNNING
                            && !metadata.getAssignments(recovered.jobId()).containsValue(killedWorkerId));
            awaitCondition("replayed Iceberg rows to commit exactly once", Duration.ofSeconds(25),
                    () -> readIceberg(recoveredTable).rows().equals(expected));
            Files.createFile(recoveredEof);
            awaitCondition("recovered transactional job to finish", Duration.ofSeconds(10),
                    () -> metadata.getJobState(recovered.jobId()).orElse(JobState.FAILED)
                            == JobState.FINISHED);
            IcebergEvidence evidence = readIceberg(recoveredTable);
            assertThat(evidence.rows()).containsExactlyElementsOf(readIceberg(cleanTable).rows());
            assertThat(evidence.rows()).doesNotHaveDuplicates();
            assertThat(evidence.dataPaths()).doesNotContainAnyElementsOf(orphans);
            for (String path : orphans) {
                assertThat(recoveredTable.io().newInputFile(path).exists())
                        .as("abandoned file remains an orphan, not a committed duplicate").isTrue();
            }
            System.out.println("ICEBERG RECOVERY: " + evidence.rows().size()
                    + " rows equal clean replay; " + orphans.size() + " unreferenced Parquet orphan(s)");
            Files.writeString(temporaryDirectory.resolve("iceberg-evidence.txt"),
                    "Clean table: " + cleanTable.name() + "\nRecovered table: " + recoveredTable.name()
                            + "\nRows (identical and unique in both tables):\n"
                            + String.join("\n", evidence.rows())
                            + "\nCommitted Parquet paths:\n" + String.join("\n", evidence.dataPaths())
                            + "\nExisting unreferenced orphan paths:\n" + String.join("\n", orphans) + "\n");
            passed = true;
        } finally {
            stopAll(processes);
            Path evidence = exportEvidence("demo-1-iceberg-worker-loss", fixture, processes, passed);
            Path details = temporaryDirectory.resolve("iceberg-evidence.txt");
            if (Files.exists(details)) {
                Files.copy(details, evidence.resolve("iceberg-evidence.txt"), StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }

    private Map<String, ManagedProcess> startRecoveryCluster(List<ManagedProcess> processes,
            EtcdMetadataStore metadata, String classpath, int masterPort,
            String etcdEndpoint, String minioEndpoint, String bucket) throws Exception {
        ManagedProcess master = startProcess("iceberg-master",
                "dev.dhruv.streaming.master.MasterBootstrap", classpath, Map.of(
                        "MASTER_PORT", Integer.toString(masterPort),
                        "MASTER_STATUS_PORT", Integer.toString(freePort()),
                        "ETCD_ENDPOINTS", etcdEndpoint,
                        "JOB_CLASSPATH", classpath,
                        "CHECKPOINT_INTERVAL_MS", "5000",
                        "CHECKPOINT_TIMEOUT_MS", "10000",
                        "RESTART_MAX_ATTEMPTS", "3",
                        "RESTART_DELAY_MS", "250"));
        processes.add(master);
        awaitPort(master, masterPort, Duration.ofSeconds(15));
        Map<String, ManagedProcess> workers = new LinkedHashMap<>();
        for (int index = 1; index <= 3; index++) {
            String workerId = "iceberg-worker-" + index;
            ManagedProcess worker = startProcess(workerId,
                    "dev.dhruv.streaming.worker.WorkerBootstrap", classpath, Map.ofEntries(
                            Map.entry("WORKER_ID", workerId),
                            Map.entry("WORKER_HOST", "127.0.0.1"),
                            Map.entry("WORKER_RPC_PORT", Integer.toString(freePort())),
                            Map.entry("WORKER_DATA_PORT", Integer.toString(freePort())),
                            Map.entry("WORKER_METRICS_PORT", Integer.toString(freePort())),
                            Map.entry("WORKER_SLOTS", "8"),
                            Map.entry("MASTER_HOST", "127.0.0.1"),
                            Map.entry("MASTER_PORT", Integer.toString(masterPort)),
                            Map.entry("ETCD_ENDPOINTS", etcdEndpoint),
                            Map.entry("JOB_CLASSPATH", classpath),
                            Map.entry("CHECKPOINT_DIR", temporaryDirectory.resolve(
                                    "iceberg-checkpoints/" + workerId).toString()),
                            Map.entry("MINIO_ENDPOINT", minioEndpoint),
                            Map.entry("MINIO_ACCESS_KEY", MINIO_USER),
                            Map.entry("MINIO_SECRET_KEY", MINIO_PASSWORD),
                            Map.entry("MINIO_BUCKET", bucket)));
            workers.put(workerId, worker);
            processes.add(worker);
        }
        awaitCondition("three Iceberg-recovery workers to register", Duration.ofSeconds(20),
                () -> metadata.listWorkers().size() == 3);
        return workers;
    }

    private static void submit(JobGraph graph, int port) throws Exception {
        try (JobClient client = new JobClient("127.0.0.1", port)) {
            assertThat(client.submit(graph, 3, 15).getAccepted()).isTrue();
        }
    }

    private static Operator<SessionRow, Void> icebergSink(String catalogUri,
            Map<String, String> properties, TableIdentifier table, Path orphanMarker) {
        IcebergSink<SessionRow> sink = IcebergSink.forRestCatalog("recovery", catalogUri,
                table.toString(), properties, row -> Map.of("canonical_row", canonicalRow(row)),
                table.name());
        return new GatedIcebergSink(sink, orphanMarker);
    }

    private static IcebergEvidence readIceberg(Table table) {
        table.refresh();
        List<String> paths = new ArrayList<>();
        List<String> rows = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                paths.add(task.file().location());
                try (CloseableIterable<Record> records = Parquet.read(table.io()
                        .newInputFile(task.file().location())).project(table.schema())
                        .createReaderFunc(fileSchema ->
                                GenericParquetReaders.buildReader(table.schema(), fileSchema)).build()) {
                    for (Record row : records) {
                        rows.add((String) row.getField("canonical_row"));
                    }
                }
            }
        } catch (IOException failure) {
            throw new IllegalStateException("cannot read Iceberg output", failure);
        }
        paths.sort(String::compareTo);
        rows.sort(String::compareTo);
        return new IcebergEvidence(List.copyOf(paths), List.copyOf(rows));
    }

    private record IcebergEvidence(List<String> dataPaths, List<String> rows) {
    }

    private static String canonicalRow(SessionRow row) {
        return String.join("|", row.memberId(), Long.toString(row.sessionStartMillis()),
                Long.toString(row.lastEventTimeMillis()), Long.toString(row.sessionEndMillis()),
                Long.toString(row.durationMillis()), Long.toString(row.clickCount()),
                String.join(",", row.searchTerms()));
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
                "member-beta|18000|21000|921000|3000|3|during-outage,first,second",
                "member-beta|2000000|2000000|2900000|0|1|future") + "\n");
        return bytes;
    }

    private static JobGraph graph(String name, Path fixture, Path release, Path output) {
        return graph(name, new GatedReplaySource(fixture, release, 3), output);
    }

    private static JobGraph graph(String name, GatedReplaySource source, Path output) {
        return graph(name, source, new CanonicalFileSink(output));
    }

    private static JobGraph graph(String name, GatedReplaySource source,
                                  Operator<SessionRow, Void> sink) {
        return graph(name, source, sink, "");
    }

    private static JobGraph graph(String name, GatedReplaySource source,
                                  Operator<SessionRow, Void> sink, String operatorPrefix) {
        JobGraph.Builder job = JobGraph.named(name);
        // Finished tasks remain available to the heartbeat until their worker exits. Distinct
        // logical names let the two sequential evidence jobs share one process harness.
        DataStream<ClickEvent> clicks = job.source(operatorPrefix + "clicks", source)
                .withEventTime(ClickEvent::eventTimeMillis, Duration.ofSeconds(5))
                .parallelism(1);
        clicks.filter(operatorPrefix + "drop-bots", new BotFilter())
                .keyBy(operatorPrefix + "by-member", ClickEvent::memberId)
                .process(operatorPrefix + "sessions", new SessionAggregator())
                .parallelism(3)
                .sink(operatorPrefix + "capture", sink)
                .parallelism(1);
        return job.build();
    }

    private Path writeFixture() throws IOException {
        Path workingDirectory = Path.of(System.getProperty("user.dir"));
        Path fixture = workingDirectory.resolve("demos/fixtures/recovery-clicks.txt");
        if (!Files.exists(fixture)) {
            fixture = workingDirectory.resolve("../demos/fixtures/recovery-clicks.txt").normalize();
        }
        if (!Files.isRegularFile(fixture)) {
            throw new IOException("missing recovery fixture: " + fixture);
        }
        return fixture.toAbsolutePath();
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

    /** Keeps the demonstrated process lifecycle inspectable after JUnit removes its temp files. */
    private static Path exportEvidence(String demo, Path fixture, List<ManagedProcess> processes,
                                       boolean passed) throws IOException {
        Path workingDirectory = Path.of(System.getProperty("user.dir"));
        Path module = Files.exists(workingDirectory.resolve("src/integrationTest"))
                ? workingDirectory : workingDirectory.resolve("lms-job");
        Path directory = module.resolve("build/demo-evidence").resolve(demo);
        Files.createDirectories(directory);
        Files.copy(fixture, directory.resolve("recovery-clicks.txt"), StandardCopyOption.REPLACE_EXISTING);
        for (ManagedProcess process : processes) {
            if (Files.exists(process.log())) {
                Files.copy(process.log(), directory.resolve(process.name() + ".log"),
                        StandardCopyOption.REPLACE_EXISTING);
            }
        }
        Files.writeString(directory.resolve("result.txt"),
                "Demo: " + demo + "\nResult: " + (passed ? "PASS" : "FAIL")
                        + "\nFixture: recovery-clicks.txt\n"
                        + "All child processes were stopped after the run.\n");
        System.out.println("Demo evidence: " + directory.toAbsolutePath());
        return directory;
    }

    private record ManagedProcess(String name, Process process, Path log) {
    }

    /** A bounded, checkpointable file source with a test-controlled pause after its prefix. */
    private static final class GatedReplaySource implements CheckpointableSource<ClickEvent> {
        private static final long serialVersionUID = 1L;

        private final String fixture;
        private final String release;
        private final long gatedAfterLines;
        private final String outageRelease;
        private final String outageProgress;
        private final String terminalRelease;
        private final long terminalAfterLines;
        private transient BufferedReader reader;
        private transient long lineNumber;

        private GatedReplaySource(Path fixture, Path release, long gatedAfterLines) {
            this(fixture, release, gatedAfterLines, null, null);
        }

        private GatedReplaySource(Path fixture, Path release, long gatedAfterLines,
                                  Path outageRelease, Path outageProgress) {
            this(fixture, release, gatedAfterLines, outageRelease, outageProgress, null, Long.MAX_VALUE);
        }

        private GatedReplaySource(Path fixture, Path release, long gatedAfterLines,
                                  Path outageRelease, Path outageProgress,
                                  Path terminalRelease, long terminalAfterLines) {
            this.fixture = fixture.toString();
            this.release = release.toString();
            this.gatedAfterLines = gatedAfterLines;
            this.outageRelease = outageRelease == null ? null : outageRelease.toString();
            this.outageProgress = outageProgress == null ? null : outageProgress.toString();
            this.terminalRelease = terminalRelease == null ? null : terminalRelease.toString();
            this.terminalAfterLines = terminalAfterLines;
        }

        @Override
        public void open(SourceContext context) throws IOException {
            reader = Files.newBufferedReader(Path.of(fixture));
            lineNumber = 0;
        }

        @Override
        public boolean poll(Collector<ClickEvent> out) throws Exception {
            requireOpen();
            if (terminalRelease != null && lineNumber >= terminalAfterLines
                    && !Files.exists(Path.of(terminalRelease))) {
                Thread.sleep(25);
                return true;
            }
            // Master-loss replay has two gates: one click can flow during the outage, while
            // the event that closes the session waits until the restarted master has restored
            // the complete job. A worker-loss replay only needs the final release gate.
            boolean outageClickReleased = outageRelease != null
                    && lineNumber == gatedAfterLines && Files.exists(Path.of(outageRelease));
            if (lineNumber >= gatedAfterLines && !Files.exists(Path.of(release))
                    && !outageClickReleased) {
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
            if (outageProgress != null && lineNumber == gatedAfterLines + 1) {
                Files.writeString(Path.of(outageProgress), Long.toString(lineNumber));
            }
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
            String encoded = canonicalRow(row) + "\n";
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

    /** Pauses a real prepared transaction once, so the process can die before its ACK. */
    private static final class GatedIcebergSink implements Operator<SessionRow, Void>, CheckpointListener {
        private static final long serialVersionUID = 1L;
        private final IcebergSink<SessionRow> delegate;
        private final String orphanMarker;

        private GatedIcebergSink(IcebergSink<SessionRow> delegate, Path orphanMarker) {
            this.delegate = delegate;
            this.orphanMarker = orphanMarker == null ? null : orphanMarker.toString();
        }

        @Override
        public void open(OperatorContext context) throws Exception {
            delegate.open(context);
        }

        @Override
        public void processElement(StreamRecord<SessionRow> record, Collector<Void> out) throws Exception {
            delegate.processElement(record, out);
        }

        @Override
        public Serializable preCommit(long checkpointId) throws Exception {
            IcebergSink.IcebergSinkState state = (IcebergSink.IcebergSinkState) delegate.preCommit(checkpointId);
            List<String> paths = state.pendingFiles().values().stream().flatMap(List::stream)
                    .map(IcebergSink.PendingDataFile::path).toList();
            if (orphanMarker != null && !paths.isEmpty() && !Files.exists(Path.of(orphanMarker))) {
                Files.write(Path.of(orphanMarker), paths, StandardCharsets.UTF_8);
                // A real failure window: the files exist, but no completed checkpoint points
                // to them. The test force-kills this process, so this thread never sends ACK.
                while (true) {
                    Thread.sleep(25);
                }
            }
            return state;
        }

        @Override
        public void restoreCheckpointState(Serializable state) throws Exception {
            delegate.restoreCheckpointState(state);
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            delegate.notifyCheckpointComplete(checkpointId);
        }

        @Override
        public void close() throws Exception {
            delegate.close();
        }
    }
}
