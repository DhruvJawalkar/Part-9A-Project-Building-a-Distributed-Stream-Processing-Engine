package dev.dhruv.streaming.master;

import dev.dhruv.streaming.api.Collector;
import dev.dhruv.streaming.api.Operator;
import dev.dhruv.streaming.api.Source;
import dev.dhruv.streaming.api.SourceContext;
import dev.dhruv.streaming.api.graph.JobGraph;
import dev.dhruv.streaming.master.graph.ExecutionGraph;
import dev.dhruv.streaming.metadata.RegisteredWorker;
import dev.dhruv.streaming.metadata.InMemoryMetadataStore;
import dev.dhruv.streaming.metadata.JobState;
import dev.dhruv.streaming.metadata.MetadataStore;
import dev.dhruv.streaming.metadata.CompletedCheckpoint;
import dev.dhruv.streaming.runtime.SerializationUtil;
import dev.dhruv.streaming.rpc.TaskId;
import dev.dhruv.streaming.rpc.TaskState;
import dev.dhruv.streaming.rpc.TaskStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the job lifecycle: submit, fail, and what a restarted master can recover.
 *
 * <p>Against the in-memory store, so that a failure here is about the state machine rather than
 * about a container. The etcd implementation is covered separately by
 * {@code EtcdMetadataStoreIT}, which is where lease expiry and watch delivery are checked.
 */
class JobMasterTest {

    private MetadataStore metadata;
    private RecordingDeployer deployer;
    private JobMaster master;

    private final List<MetadataStore.WorkerRegistration> registrations = new ArrayList<>();

    @BeforeEach
    void setUp() {
        metadata = new InMemoryMetadataStore();
        deployer = new RecordingDeployer();
        master = new JobMaster(metadata, deployer);
        registerWorkers("worker-1", "worker-2", "worker-3");
    }

    @AfterEach
    void tearDown() {
        registrations.forEach(MetadataStore.WorkerRegistration::close);
        master.close();
        metadata.close();
    }

    @Test
    @DisplayName("submitting drives CREATED then RUNNING, and deploys")
    void submitReachesRunning() {
        JobGraph graph = lmsShapedJob();

        ExecutionGraph plan = master.submit(graph, "graph-bytes".getBytes());

        assertThat(master.stateOf(graph.jobId())).contains(JobState.RUNNING);
        assertThat(deployer.deployed).hasSize(1);
        assertThat(plan.taskCount()).isEqualTo(6);
    }

    @Test
    @DisplayName("a rejected initial deployment is recorded as failed, not left RUNNING")
    void initialDeploymentFailureIsTerminal() {
        JobGraph graph = lmsShapedJob();
        deployer.failInitialDeployment = true;

        assertThatThrownBy(() -> master.submit(graph, "graph-bytes".getBytes()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("simulated initial deployment failure");

        assertThat(master.stateOf(graph.jobId())).contains(JobState.FAILED);
        assertThat(metadata.getJobFailureCause(graph.jobId()))
                .contains("initial deployment failed: simulated initial deployment failure");
        assertThat(deployer.cancelledJobs).containsExactly(graph.jobId());
    }

    @Test
    @DisplayName("distributes a two-chain job across all three workers")
    void jobOccupiesEveryWorker() {
        // The acceptance criterion. Two chain groups over three workers only fills the cluster
        // because the unit of scheduling is a vertical slice rather than a whole group.
        ExecutionGraph plan = master.submit(lmsShapedJob(), "graph-bytes".getBytes());

        assertThat(plan.workerIds()).containsExactly("worker-1", "worker-2", "worker-3");
    }

    @Test
    @DisplayName("persists the graph and assignments before deploying anything")
    void persistsBeforeDeploying() {
        JobGraph graph = lmsShapedJob();

        master.submit(graph, "graph-bytes".getBytes());

        assertThat(metadata.getJobGraph(graph.jobId())).isPresent();
        assertThat(metadata.getAssignments(graph.jobId())).hasSize(6);
    }

    @Test
    @DisplayName("a bounded job finishes after every physical task reports finished")
    void boundedJobFinishesFromHeartbeatTaskStates() {
        JobGraph graph = lmsShapedJob();
        ExecutionGraph plan = master.submit(graph, SerializationUtil.toBytes(graph));

        plan.assignments().keySet().forEach(key -> {
            int separator = key.lastIndexOf(':');
            TaskStatus status = TaskStatus.newBuilder()
                    .setTaskId(TaskId.newBuilder()
                            .setJobId(graph.jobId())
                            .setOperatorId(key.substring(0, separator))
                            .setSubtaskIndex(Integer.parseInt(key.substring(separator + 1))))
                    .setState(TaskState.TASK_FINISHED)
                    .setRecordsIn(7)
                    .build();
            master.onTaskStatus(status);
        });

        assertThat(master.stateOf(graph.jobId())).contains(JobState.FINISHED);
        assertThat(master.taskStatuses(graph.jobId())).hasSize(plan.taskCount());
        assertThat(master.taskStatuses(graph.jobId()).values())
                .allMatch(status -> status.getRecordsIn() == 7);
    }

    @Test
    @DisplayName("a dead worker fails the job, with the cause in the store")
    void workerDeathFailsTheJob() {
        JobGraph graph = lmsShapedJob();
        master.submit(graph, "graph-bytes".getBytes());

        master.taskTracker().workerRegistered("worker-2");
        master.failJob(graph.jobId(), "worker worker-2 died holding 2 task(s)");

        assertThat(master.stateOf(graph.jobId())).contains(JobState.FAILED);
        assertThat(metadata.getJobFailureCause(graph.jobId()))
                .get().asString().contains("worker-2");
        assertThat(deployer.cancelledJobs).contains(graph.jobId());
    }

    @Test
    @DisplayName("a task failure fails the whole job")
    void taskFailureFailsTheJob() {
        // Phase 2 has nothing to recover to, so one task failing is terminal for the job. Phase
        // 4 replaces this with a restart from the last completed checkpoint.
        JobGraph graph = lmsShapedJob();
        master.submit(graph, "graph-bytes".getBytes());

        master.onTaskFailure(graph.jobId(), "drop-bots:2", "NullPointerException");

        assertThat(master.stateOf(graph.jobId())).contains(JobState.FAILED);
        assertThat(metadata.getJobFailureCause(graph.jobId()))
                .get().asString().contains("drop-bots:2");
    }

    @Test
    @DisplayName("failing twice does not re-cancel")
    void failingIsIdempotent() {
        JobGraph graph = lmsShapedJob();
        master.submit(graph, "graph-bytes".getBytes());

        master.failJob(graph.jobId(), "first");
        master.failJob(graph.jobId(), "second");

        assertThat(metadata.getJobFailureCause(graph.jobId())).contains("first");
        assertThat(deployer.cancelledJobs).hasSize(1);
    }

    @Test
    @DisplayName("a restarted master recovers graph, assignments and state")
    void restartedMasterRecovers() {
        // The acceptance criterion: a master that dies must come back knowing what it was
        // doing, rather than losing the job. Everything it needs is in the store, so a fresh
        // JobMaster over the same store is exactly what a restart looks like.
        JobGraph graph = lmsShapedJob();
        master.submit(graph, "graph-bytes".getBytes());
        master.close();

        JobMaster restarted = new JobMaster(metadata, new RecordingDeployer());
        try {
            var recovered = restarted.recover();

            assertThat(recovered).containsKey(graph.jobId());
            JobMaster.RecoveredJob job = recovered.get(graph.jobId());
            assertThat(job.state()).isEqualTo(JobState.RUNNING);
            assertThat(job.assignments()).hasSize(6);
            assertThat(job.graphBytes()).isGreaterThan(0);
        } finally {
            restarted.close();
        }
    }

    @Test
    @DisplayName("a restarted master sees why a job failed")
    void restartedMasterSeesFailureCause() {
        JobGraph graph = lmsShapedJob();
        master.submit(graph, "graph-bytes".getBytes());
        master.failJob(graph.jobId(), "worker worker-2 died");
        master.close();

        JobMaster restarted = new JobMaster(metadata, new RecordingDeployer());
        try {
            JobMaster.RecoveredJob job = restarted.recover().get(graph.jobId());

            assertThat(job.state()).isEqualTo(JobState.FAILED);
            assertThat(job.failureCause()).get().asString().contains("worker-2");
        } finally {
            restarted.close();
        }
    }

    @Test
    @DisplayName("a restarted master rebuilds its plan without deploying a second copy")
    void restartedMasterRebuildsPlanForLaterFailureRecovery() {
        JobGraph graph = lmsShapedJob();
        master.submit(graph, SerializationUtil.toBytes(graph));
        master.close();

        RecordingDeployer recoveredDeployer = new RecordingDeployer();
        JobMaster restarted = new JobMaster(metadata, recoveredDeployer,
                new CheckpointCoordinator.Config(Duration.ofDays(1), Duration.ofSeconds(1)),
                RestartStrategy.fixedDelayDefault());
        try {
            restarted.recover();

            assertThat(restarted.planOf(graph.jobId())).isPresent();
            assertThat(recoveredDeployer.deployed).isEmpty();
        } finally {
            restarted.close();
        }
    }

    @Test
    @DisplayName("a restarted master resumes a redeploy interrupted in RESTARTING")
    void restartedMasterResumesInterruptedRecovery() throws InterruptedException {
        JobGraph graph = lmsShapedJob();
        ExecutionGraph plan = master.submit(graph, SerializationUtil.toBytes(graph));
        metadata.putLatestCompletedCheckpoint(graph.jobId(), checkpointFor(plan));
        metadata.putJobState(graph.jobId(), JobState.RESTARTING);
        master.close();

        RecordingDeployer recoveredDeployer = new RecordingDeployer();
        JobMaster restarted = new JobMaster(metadata, recoveredDeployer,
                new CheckpointCoordinator.Config(Duration.ofDays(1), Duration.ofSeconds(1)),
                new RestartStrategy(1, Duration.ZERO));
        try {
            restarted.recover();

            assertThat(recoveredDeployer.restarted.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(awaitState(restarted, graph.jobId(), JobState.RUNNING)).isTrue();
        } finally {
            restarted.close();
        }
    }

    @Test
    @DisplayName("a restarted master reconciles FAILING before it redeploys")
    void restartedMasterReconcilesInterruptedFailure() throws InterruptedException {
        JobGraph graph = lmsShapedJob();
        ExecutionGraph plan = master.submit(graph, SerializationUtil.toBytes(graph));
        metadata.putLatestCompletedCheckpoint(graph.jobId(), checkpointFor(plan));
        metadata.putJobState(graph.jobId(), JobState.FAILING);
        master.close();

        RecordingDeployer recoveredDeployer = new RecordingDeployer();
        JobMaster restarted = new JobMaster(metadata, recoveredDeployer,
                new CheckpointCoordinator.Config(Duration.ofDays(1), Duration.ofSeconds(1)),
                new RestartStrategy(1, Duration.ZERO));
        try {
            restarted.recover();

            assertThat(recoveredDeployer.cancelledJobs).contains(graph.jobId());
            assertThat(recoveredDeployer.restarted.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(awaitState(restarted, graph.jobId(), JobState.RUNNING)).isTrue();
        } finally {
            restarted.close();
        }
    }

    @Test
    @DisplayName("a task failure restarts every task from one completed checkpoint")
    void taskFailureRestartsWholeJobFromCheckpoint() throws InterruptedException {
        master.close();
        deployer = new RecordingDeployer();
        master = new JobMaster(metadata, deployer,
                new CheckpointCoordinator.Config(Duration.ofDays(1), Duration.ofSeconds(1)),
                new RestartStrategy(1, Duration.ZERO));
        JobGraph graph = lmsShapedJob();
        ExecutionGraph initialPlan = master.submit(graph, "graph-bytes".getBytes());
        metadata.putLatestCompletedCheckpoint(graph.jobId(), checkpointFor(initialPlan));

        master.onTaskFailure(graph.jobId(), "drop-bots:2", "simulated worker loss");

        assertThat(deployer.restarted.await(1, TimeUnit.SECONDS)).isTrue();
        assertThat(awaitState(graph.jobId(), JobState.RUNNING)).isTrue();
        assertThat(deployer.cancelledJobs).contains(graph.jobId());
        assertThat(deployer.recoveredDeployments).contains(graph.jobId());
        assertThat(deployer.lastRecoveryHandles).hasSize(initialPlan.taskCount());
    }

    @Test
    @DisplayName("recovery does not reschedule onto a dead worker whose lease is still visible")
    void recoveryQuarantinesDeadWorkerBeforeItsLeaseExpires() throws InterruptedException {
        master.close();
        deployer = new RecordingDeployer();
        master = new JobMaster(metadata, deployer,
                new CheckpointCoordinator.Config(Duration.ofDays(1), Duration.ofSeconds(1)),
                new RestartStrategy(1, Duration.ZERO));
        JobGraph graph = lmsShapedJob();
        ExecutionGraph plan = master.submit(graph, SerializationUtil.toBytes(graph));
        metadata.putLatestCompletedCheckpoint(graph.jobId(), checkpointFor(plan));

        master.onWorkerDead("worker-2");

        assertThat(deployer.restarted.await(1, TimeUnit.SECONDS)).isTrue();
        assertThat(deployer.lastRecoveryWorkers).doesNotContain("worker-2");
        assertThat(master.planOf(graph.jobId()).orElseThrow().workerIds())
                .doesNotContain("worker-2");
    }

    @Test
    @DisplayName("recovery stops at the configured restart-attempt limit")
    void restartAttemptsEventuallyBecomeTerminal() throws InterruptedException {
        master.close();
        deployer = new RecordingDeployer();
        deployer.failRecoveredDeployment = true;
        master = new JobMaster(metadata, deployer,
                new CheckpointCoordinator.Config(Duration.ofDays(1), Duration.ofSeconds(1)),
                new RestartStrategy(1, Duration.ZERO));
        JobGraph graph = lmsShapedJob();
        ExecutionGraph plan = master.submit(graph, "graph-bytes".getBytes());
        metadata.putLatestCompletedCheckpoint(graph.jobId(), checkpointFor(plan));

        master.onTaskFailure(graph.jobId(), "drop-bots:2", "first failure");

        assertThat(deployer.restarted.await(1, TimeUnit.SECONDS)).isTrue();
        assertThat(awaitState(graph.jobId(), JobState.FAILED)).isTrue();
    }

    // -------------------------------------------------------------------------------------

    private void registerWorkers(String... workerIds) {
        int port = 9091;
        for (String workerId : workerIds) {
            registrations.add(metadata.registerWorker(
                    new RegisteredWorker(workerId, "10.0.0." + port, port, port + 100, 8), 10));
        }
    }

    private static JobGraph lmsShapedJob() {
        JobGraph.Builder job = JobGraph.named("lms-clickstream");
        job.source("clicks", noopSource()).parallelism(4)
                .filter("drop-bots", value -> true).parallelism(4)
                .sink("console", printSink()).parallelism(2);
        return job.build();
    }

    private static Operator<String, Void> printSink() {
        return (record, out) -> {
        };
    }

    private static Source<String> noopSource() {
        return new Source<>() {
            @Override
            public void open(SourceContext context) {
            }

            @Override
            public boolean poll(Collector<String> out) {
                return false;
            }

            @Override
            public void close() {
            }
        };
    }

    private static CompletedCheckpoint checkpointFor(ExecutionGraph plan) {
        Map<String, CompletedCheckpoint.TaskState> handles = new java.util.LinkedHashMap<>();
        plan.chainGroups().forEach(chain -> {
            for (int subtask = 0; subtask < chain.parallelism(); subtask++) {
                handles.put(chain.head().id() + ":" + subtask,
                        new CompletedCheckpoint.TaskState("file:///checkpoint/"
                                + chain.head().id() + '-' + subtask, 10, 0));
            }
        });
        return new CompletedCheckpoint(1, 1234, handles);
    }

    private boolean awaitState(String jobId, JobState expected) throws InterruptedException {
        return awaitState(master, jobId, expected);
    }

    private static boolean awaitState(JobMaster jobMaster, String jobId, JobState expected)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        while (System.nanoTime() < deadline) {
            if (jobMaster.stateOf(jobId).orElse(null) == expected) {
                return true;
            }
            Thread.sleep(10);
        }
        return jobMaster.stateOf(jobId).orElse(null) == expected;
    }

    /** Records what it was asked to do, so the master's decisions can be asserted on. */
    private static final class RecordingDeployer implements TaskDeployer {

        private final List<String> deployed = new ArrayList<>();
        private final List<String> cancelledJobs = new ArrayList<>();
        private final List<String> recoveredDeployments = new ArrayList<>();
        private final CountDownLatch restarted = new CountDownLatch(1);
        private Map<String, String> lastRecoveryHandles = Map.of();
        private List<String> lastRecoveryWorkers = List.of();
        private boolean failRecoveredDeployment;
        private boolean failInitialDeployment;

        @Override
        public void deploy(JobGraph graph, ExecutionGraph plan, List<RegisteredWorker> workers) {
            if (failInitialDeployment) {
                throw new IllegalStateException("simulated initial deployment failure");
            }
            deployed.add(graph.jobId());
        }

        @Override
        public void cancelAll(String jobId, ExecutionGraph plan) {
            cancelledJobs.add(jobId);
        }

        @Override
        public void deployFromCheckpoint(JobGraph graph,
                                         ExecutionGraph plan,
                                         List<RegisteredWorker> workers,
                                         Map<String, String> stateHandles) {
            recoveredDeployments.add(graph.jobId());
            lastRecoveryHandles = Map.copyOf(stateHandles);
            lastRecoveryWorkers = workers.stream().map(RegisteredWorker::workerId).toList();
            restarted.countDown();
            if (failRecoveredDeployment) {
                throw new IllegalStateException("simulated redeploy failure");
            }
        }
    }
}
