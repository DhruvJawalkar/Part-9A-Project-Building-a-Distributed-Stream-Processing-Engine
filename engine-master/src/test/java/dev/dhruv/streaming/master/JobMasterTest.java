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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

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

    /** Records what it was asked to do, so the master's decisions can be asserted on. */
    private static final class RecordingDeployer implements TaskDeployer {

        private final List<String> deployed = new ArrayList<>();
        private final List<String> cancelledJobs = new ArrayList<>();

        @Override
        public void deploy(JobGraph graph, ExecutionGraph plan, List<RegisteredWorker> workers) {
            deployed.add(graph.jobId());
        }

        @Override
        public void cancelAll(String jobId, ExecutionGraph plan) {
            cancelledJobs.add(jobId);
        }
    }
}
