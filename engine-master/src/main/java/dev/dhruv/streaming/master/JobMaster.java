package dev.dhruv.streaming.master;

import dev.dhruv.streaming.api.graph.JobGraph;
import dev.dhruv.streaming.master.graph.ExecutionGraph;
import dev.dhruv.streaming.master.graph.ExecutionGraphCompiler;
import dev.dhruv.streaming.metadata.RegisteredWorker;
import dev.dhruv.streaming.master.graph.TaskAssignment;
import dev.dhruv.streaming.metadata.JobState;
import dev.dhruv.streaming.metadata.MetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Owns one job: compiles it, schedules it, watches it, and decides when it has failed.
 *
 * <h2>Write first, then act</h2>
 *
 * <p>Every state transition is persisted <em>before</em> the actions that transition implies.
 * The order is the whole point, and it is easiest to see by inverting it: a master that
 * cancelled a job's tasks and then crashed before recording why would come back to find tasks
 * gone from a job its own records still describe as {@code RUNNING}. It could not tell that
 * from a job whose workers had merely not reported yet.
 *
 * <p>Writing first makes the crash window harmless. A master that dies after recording
 * {@code FAILING} but before cancelling anything comes back knowing exactly what it was in the
 * middle of, and can finish the job it started.
 *
 * <h2>Phase 2 fails; Phase 4 recovers</h2>
 *
 * <p>When a worker dies here, the job dies with it. That is not a simplification being taken on
 * the way to something better -- it is the correct response given what exists. The dead worker
 * held tasks with in-flight records and no snapshot of anything, so there is no consistent point
 * for the rest of the job to be rewound to. Phase 4 creates such a point, and only then does
 * restarting become meaningful.
 */
public final class JobMaster implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(JobMaster.class);

    private final MetadataStore metadata;
    private final TaskTracker taskTracker;
    private final TaskDeployer deployer;

    private final Map<String, JobGraph> jobGraphs = new ConcurrentHashMap<>();
    private final Map<String, ExecutionGraph> executionGraphs = new ConcurrentHashMap<>();

    /**
     * Creates a master.
     *
     * @param metadata where durable job state lives
     * @param deployer how tasks are actually sent to workers
     */
    public JobMaster(MetadataStore metadata, TaskDeployer deployer) {
        this.metadata = metadata;
        this.deployer = deployer;
        this.taskTracker = new TaskTracker(this::onWorkerDead);
    }

    /**
     * Returns the tracker, so the gRPC service can feed it heartbeats.
     *
     * @return the task tracker
     */
    public TaskTracker taskTracker() {
        return taskTracker;
    }

    /**
     * Submits a job: persist it, compile it, record the plan, then deploy.
     *
     * <p>The graph is written before it is compiled, because a job that exists in etcd but was
     * never scheduled is recoverable, while one that was scheduled but never recorded is a set
     * of orphaned tasks no master knows about.
     *
     * @param graph           the validated logical graph
     * @param serializedGraph the same graph, serialized for etcd
     * @return the physical plan
     * @throws IllegalStateException if the cluster cannot accommodate the job
     */
    public ExecutionGraph submit(JobGraph graph, byte[] serializedGraph) {
        String jobId = graph.jobId();
        log.info("submitting job '{}' ({})", graph.name(), jobId);

        metadata.putJobGraph(jobId, serializedGraph);
        metadata.putJobState(jobId, JobState.CREATED);
        jobGraphs.put(jobId, graph);

        List<RegisteredWorker> workers = metadata.listWorkers();
        ExecutionGraph plan = ExecutionGraphCompiler.compile(graph, workers);
        executionGraphs.put(jobId, plan);

        Map<String, String> assignments = new LinkedHashMap<>();
        plan.assignments().forEach((key, assignment) ->
                assignments.put(key, assignment.workerId()));
        metadata.putAssignments(jobId, assignments);

        // RUNNING before deploying, not after. A task that starts processing under a master
        // that still believes the job is CREATED is a task nobody is watching.
        metadata.putJobState(jobId, JobState.RUNNING);
        deployer.deploy(graph, plan, workers);

        log.info("job '{}' running: {} tasks across {} workers",
                graph.name(), plan.taskCount(), plan.workerIds().size());
        return plan;
    }

    /**
     * Fails a job, recording the cause before acting on it.
     *
     * @param jobId  the job
     * @param cause  why, in terms a person reading etcd would understand
     */
    public void failJob(String jobId, String cause) {
        Optional<JobState> current = metadata.getJobState(jobId);
        if (current.isEmpty() || current.get().isTerminal()) {
            return;                         // already finished or already failed
        }

        log.error("failing job {}: {}", jobId, cause);

        // Cause before state, state before cancellation. Each write is only useful if it
        // precedes what it describes.
        metadata.putJobFailureCause(jobId, cause);
        metadata.putJobState(jobId, JobState.FAILING);

        ExecutionGraph plan = executionGraphs.get(jobId);
        if (plan != null) {
            deployer.cancelAll(jobId, plan);
        }

        metadata.putJobState(jobId, JobState.FAILED);
    }

    /**
     * Marks a job finished, which only a bounded source ever reaches.
     *
     * @param jobId the job
     */
    public void finishJob(String jobId) {
        Optional<JobState> current = metadata.getJobState(jobId);
        if (current.isPresent() && !current.get().isTerminal()) {
            metadata.putJobState(jobId, JobState.FINISHED);
            log.info("job {} finished", jobId);
        }
    }

    /**
     * Called when a task reports that it failed.
     *
     * @param jobId   the job the task belonged to
     * @param taskId  which task
     * @param message what went wrong
     */
    public void onTaskFailure(String jobId, String taskId, String message) {
        failJob(jobId, "task " + taskId + " failed: " + message);
    }

    /**
     * Called by the tracker when a worker stops beating.
     *
     * <p>Every job with a task on that worker fails. There is no partial recovery available: the
     * lost tasks held records that were in flight and state that was never snapshotted, so the
     * surviving tasks have already moved past any point the lost ones could be restarted from.
     */
    private void onWorkerDead(String workerId) {
        executionGraphs.forEach((jobId, plan) -> {
            List<TaskAssignment> lost = plan.assignmentsFor(workerId);
            if (!lost.isEmpty()) {
                failJob(jobId, "worker " + workerId + " died holding " + lost.size()
                        + " task(s): " + lost.stream().map(TaskAssignment::key).toList());
            }
        });
    }

    /**
     * Recovers what this master was doing, after a restart.
     *
     * <p>Reads back every job it knew about, so that a restarted master can distinguish a job it
     * should be watching from one that ended. Phase 4 extends this into genuine recovery: read
     * the latest checkpoint pointer and redeploy from it. Here it is only the reading, which is
     * what makes the acceptance criterion about a restarted master meaningful.
     *
     * @return what was found, by job id
     */
    public Map<String, RecoveredJob> recover() {
        Map<String, RecoveredJob> recovered = new LinkedHashMap<>();
        for (String jobId : metadata.listJobs()) {
            JobState state = metadata.getJobState(jobId).orElse(JobState.CREATED);
            Map<String, String> assignments = metadata.getAssignments(jobId);
            Optional<byte[]> graph = metadata.getJobGraph(jobId);
            Optional<String> cause = metadata.getJobFailureCause(jobId);

            recovered.put(jobId, new RecoveredJob(jobId, state, assignments,
                    graph.map(bytes -> bytes.length).orElse(0), cause));
            log.info("recovered job {} in state {} with {} assignments",
                    jobId, state, assignments.size());
        }
        return recovered;
    }

    /**
     * Returns a job's current state.
     *
     * @param jobId the job
     * @return its state, if known
     */
    public Optional<JobState> stateOf(String jobId) {
        return metadata.getJobState(jobId);
    }

    /**
     * Returns a job's compiled plan.
     *
     * @param jobId the job
     * @return the plan, if this master compiled it
     */
    public Optional<ExecutionGraph> planOf(String jobId) {
        return Optional.ofNullable(executionGraphs.get(jobId));
    }

    @Override
    public void close() {
        taskTracker.close();
    }

    /**
     * What a restarted master found in etcd about one job.
     *
     * @param jobId        the job
     * @param state        what it was doing
     * @param assignments  where its tasks were placed
     * @param graphBytes   size of the stored graph, enough to show it survived
     * @param failureCause why it failed, if it did
     */
    public record RecoveredJob(
            String jobId,
            JobState state,
            Map<String, String> assignments,
            int graphBytes,
            Optional<String> failureCause
    ) {
    }
}
