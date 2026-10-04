package dev.dhruv.streaming.master;

import dev.dhruv.streaming.api.graph.JobGraph;
import dev.dhruv.streaming.master.graph.ExecutionGraph;
import dev.dhruv.streaming.master.graph.ExecutionGraphCompiler;
import dev.dhruv.streaming.metadata.RegisteredWorker;
import dev.dhruv.streaming.master.graph.TaskAssignment;
import dev.dhruv.streaming.metadata.JobState;
import dev.dhruv.streaming.metadata.MetadataStore;
import dev.dhruv.streaming.metadata.CompletedCheckpoint;
import dev.dhruv.streaming.runtime.SerializationUtil;
import dev.dhruv.streaming.rpc.CheckpointAck;
import dev.dhruv.streaming.rpc.TaskState;
import dev.dhruv.streaming.rpc.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;

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
 * <h2>Phase 4 recovers the whole job</h2>
 *
 * <p>A failure restarts every task, not just the task that reported it. This can look wasteful,
 * but a task-specific restart is wrong: surviving tasks may already have consumed records that
 * the recovered task will replay from its snapshot. Rewinding every task to one completed
 * checkpoint is what makes their input/output boundary a consistent cut again.
 */
public final class JobMaster implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(JobMaster.class);

    private final MetadataStore metadata;
    private final TaskTracker taskTracker;
    private final TaskDeployer deployer;
    private final CheckpointCoordinator checkpoints;
    private final RestartStrategy restartStrategy;
    private final ScheduledExecutorService recoveryScheduler;

    private final Map<String, JobGraph> jobGraphs = new ConcurrentHashMap<>();
    private final Map<String, ExecutionGraph> executionGraphs = new ConcurrentHashMap<>();
    private final Map<String, Integer> restartAttempts = new ConcurrentHashMap<>();
    private final Map<String, Instant> startedAt = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> finishedTasks = new ConcurrentHashMap<>();
    private final Map<String, Map<String, TaskStatus>> taskStatuses = new ConcurrentHashMap<>();
    private final Map<String, DeferredRecovery> deferredRecoveries = new LinkedHashMap<>();
    private final Set<String> quarantinedWorkers = ConcurrentHashMap.newKeySet();

    /**
     * Creates a master.
     *
     * @param metadata where durable job state lives
     * @param deployer how tasks are actually sent to workers
     */
    public JobMaster(MetadataStore metadata, TaskDeployer deployer) {
        this(metadata, deployer, new CheckpointCoordinator.Config(Duration.ofSeconds(10),
                Duration.ofSeconds(30)), RestartStrategy.fixedDelayDefault());
    }

    /** Creates a master with an explicit checkpoint cadence and restart policy. */
    public JobMaster(MetadataStore metadata,
                     TaskDeployer deployer,
                     CheckpointCoordinator.Config checkpointConfig,
                     RestartStrategy restartStrategy) {
        this.metadata = metadata;
        this.deployer = deployer;
        this.taskTracker = new TaskTracker(this::onWorkerDead);
        this.checkpoints = new CheckpointCoordinator(metadata, checkpointConfig);
        this.restartStrategy = restartStrategy;
        this.recoveryScheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "job-recovery");
            thread.setDaemon(true);
            return thread;
        });
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
     * Admits a newly registered worker back into scheduling.
     *
     * <p>A worker id declared dead remains in etcd until its lease expires. Quarantining it at
     * the failure detector boundary prevents the faster restart loop from immediately assigning
     * recovered tasks back to that stale registration. A process using the same stable id is
     * eligible again only after it performs a fresh control-plane registration.
     */
    public synchronized void workerRegistered(String workerId) {
        quarantinedWorkers.remove(workerId);
        taskTracker.workerRegistered(workerId);
        resumeDeferredRecoveries();
    }

    /**
     * Retries startup recovery when durable worker leases change. The worker opens its heartbeat
     * before advertising its lease, so the earlier registration RPC alone cannot see new capacity.
     */
    public synchronized void workerAvailabilityChanged() {
        resumeDeferredRecoveries();
    }

    /** Caller holds this master's monitor, shared with recover() and control registration. */
    private void resumeDeferredRecoveries() {
        if (!deferredRecoveries.isEmpty()) {
            List<RegisteredWorker> workers = schedulableWorkers();
            for (Map.Entry<String, DeferredRecovery> entry : List.copyOf(deferredRecoveries.entrySet())) {
                String jobId = entry.getKey();
                JobState state = metadata.getJobState(jobId).orElse(JobState.FAILED);
                if (state.isTerminal()) {
                    deferredRecoveries.remove(jobId);
                    continue;
                }
                DeferredRecovery recovery = entry.getValue();
                restoreRecoveredPlan(jobId, recovery.graph(), recovery.assignments(), workers, state);
            }
        }
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
    public synchronized ExecutionGraph submit(JobGraph graph, byte[] serializedGraph) {
        String jobId = graph.jobId();
        log.info("submitting job '{}' ({})", graph.name(), jobId);

        metadata.putJobGraph(jobId, serializedGraph);
        metadata.putJobState(jobId, JobState.CREATED);
        jobGraphs.put(jobId, graph);

        List<RegisteredWorker> workers = schedulableWorkers();
        ExecutionGraph plan = ExecutionGraphCompiler.compile(graph, workers);
        executionGraphs.put(jobId, plan);
        finishedTasks.put(jobId, ConcurrentHashMap.newKeySet());
        taskStatuses.put(jobId, new ConcurrentHashMap<>());

        Map<String, String> assignments = new LinkedHashMap<>();
        plan.assignments().forEach((key, assignment) ->
                assignments.put(key, assignment.workerId()));
        metadata.putAssignments(jobId, assignments);

        // RUNNING before deploying, not after. A task that starts processing under a master
        // that still believes the job is CREATED is a task nobody is watching.
        metadata.putJobState(jobId, JobState.RUNNING);
        startedAt.put(jobId, Instant.now());
        try {
            deployer.deploy(graph, plan, workers);
        } catch (RuntimeException deploymentFailure) {
            // The concrete deployer rolls back every RPC it attempted, including an uncertain
            // request whose worker may have started the task before the response was lost.
            // Record the failed admission as terminal so a restarted master never mistakes an
            // empty/partial generation for a healthy RUNNING job.
            failJob(jobId, "initial deployment failed: " + deploymentFailure.getMessage());
            throw deploymentFailure;
        }
        startCheckpointing(graph, plan);

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
    public synchronized void failJob(String jobId, String cause) {
        Optional<JobState> current = metadata.getJobState(jobId);
        if (current.isEmpty() || current.get().isTerminal()
                || current.get() == JobState.FAILING || current.get() == JobState.RESTARTING) {
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
        checkpoints.stop(jobId);

        Optional<CompletedCheckpoint> recoveryPoint = metadata.getLatestCompletedCheckpoint(jobId);
        int nextAttempt = restartAttempts.merge(jobId, 1, Integer::sum);
        if (recoveryPoint.isEmpty()) {
            metadata.putJobState(jobId, JobState.FAILED);
            log.error("job {} cannot recover: it has no completed checkpoint", jobId);
            return;
        }
        if (nextAttempt > restartStrategy.maxAttempts()) {
            metadata.putJobState(jobId, JobState.FAILED);
            log.error("job {} exhausted {} restart attempt(s)", jobId,
                    restartStrategy.maxAttempts());
            return;
        }

        metadata.putJobState(jobId, JobState.RESTARTING);
        log.warn("job {} restarting from checkpoint {} (attempt {}/{})", jobId,
                recoveryPoint.get().checkpointId(), nextAttempt, restartStrategy.maxAttempts());
        recoveryScheduler.schedule(() -> restartFromCheckpoint(jobId, recoveryPoint.get()),
                restartStrategy.delay().toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Marks a job finished, which only a bounded source ever reaches.
     *
     * @param jobId the job
     */
    public synchronized void finishJob(String jobId) {
        Optional<JobState> current = metadata.getJobState(jobId);
        if (current.isPresent() && !current.get().isTerminal()) {
            metadata.putJobState(jobId, JobState.FINISHED);
            checkpoints.stop(jobId);
            ExecutionGraph plan = executionGraphs.get(jobId);
            if (plan != null) {
                try {
                    // Finished threads still own input registrations and periodic flushers.
                    // Release them before another submission reuses the same operator names.
                    deployer.cancelAll(jobId, plan);
                } catch (RuntimeException cleanupFailure) {
                    // Completion is already durable: cleanup cannot turn processed input into
                    // a replay or roll a successful job back to FAILED/RESTARTING.
                    log.warn("job {} finished but worker resource cleanup failed", jobId,
                            cleanupFailure);
                }
            }
            log.info("job {} finished", jobId);
        }
    }

    /** Records one heartbeat status and completes a bounded job after every physical task ends. */
    public void onTaskStatus(TaskStatus status) {
        String jobId = status.getTaskId().getJobId();
        ExecutionGraph plan = executionGraphs.get(jobId);
        if (jobId.isBlank() || plan == null) {
            return;
        }
        String taskKey = status.getTaskId().getOperatorId() + ':'
                + status.getTaskId().getSubtaskIndex();
        if (!plan.assignments().containsKey(taskKey)) {
            return;
        }
        taskStatuses.computeIfAbsent(jobId, ignored -> new ConcurrentHashMap<>())
                .put(taskKey, status);
        Set<String> finished = finishedTasks.computeIfAbsent(jobId,
                ignored -> ConcurrentHashMap.newKeySet());
        if (status.getState() == TaskState.TASK_FINISHED) {
            finished.add(taskKey);
        } else {
            finished.remove(taskKey);
        }
        if (finished.size() == plan.taskCount()
                && metadata.getJobState(jobId).orElse(JobState.FAILED) == JobState.RUNNING) {
            finishJob(jobId);
        }
    }

    /** Latest per-task heartbeat samples, retained for the Phase 7 status API. */
    public Map<String, TaskStatus> taskStatuses(String jobId) {
        return Map.copyOf(taskStatuses.getOrDefault(jobId, Map.of()));
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

    /** Routes a worker acknowledgement into the coordinator's exact per-task collection. */
    public void acknowledgeCheckpoint(CheckpointAck ack) {
        checkpoints.acknowledge(ack.getTaskId().getJobId(),
                new CheckpointCoordinator.TaskKey(ack.getTaskId().getOperatorId(),
                        ack.getTaskId().getSubtaskIndex()),
                ack.getCheckpointId(), ack.getStateHandleUri(), ack.getAlignmentMillis(),
                ack.getStateSizeBytes());
    }

    /** Exposes one deterministic trigger for tests and demo scripts. */
    public Optional<Long> triggerCheckpoint(String jobId) {
        return checkpoints.trigger(jobId);
    }

    /**
     * Stops a job at an operator's request without pretending that cancellation is a failure.
     *
     * <p>Cancellation is deliberately terminal. A restart would contradict the caller's request
     * and, unlike failure recovery, has no reason to preserve an in-flight checkpoint.
     *
     * @return false when no live job by this id exists
     */
    public synchronized boolean cancelJob(String jobId) {
        Optional<JobState> current = metadata.getJobState(jobId);
        if (current.isEmpty() || current.get().isTerminal()) {
            return false;
        }
        metadata.putJobState(jobId, JobState.CANCELLED);
        ExecutionGraph plan = executionGraphs.get(jobId);
        if (plan != null) {
            deployer.cancelAll(jobId, plan);
        }
        checkpoints.stop(jobId);
        log.info("job {} cancelled through the status API", jobId);
        return true;
    }

    /** Returns the id of the one incomplete checkpoint, if the coordinator currently has one. */
    public Optional<Long> inFlightCheckpoint(String jobId) {
        return checkpoints.inFlightCheckpoint(jobId);
    }

    /** Number of whole-job recovery attempts made since this master started supervising the job. */
    public int restartCount(String jobId) {
        return restartAttempts.getOrDefault(jobId, 0);
    }

    /** Checkpoint cadence used by the status API. */
    public long checkpointIntervalMillis() {
        return checkpoints.intervalMillis();
    }

    /** Current coordinator counters for one job; they are deliberately process-local metrics. */
    public CheckpointCoordinator.Status checkpointStatus(String jobId) {
        return checkpoints.status(jobId);
    }

    /** Logical job name when this master has its graph in memory. */
    public Optional<String> jobName(String jobId) {
        return Optional.ofNullable(jobGraphs.get(jobId)).map(JobGraph::name);
    }

    /** Time this master admitted the currently running job generation, if known. */
    public Optional<Instant> startedAt(String jobId) {
        return Optional.ofNullable(startedAt.get(jobId));
    }

    /**
     * Called by the tracker when a worker stops beating.
     *
     * <p>Every job with a task on that worker fails. There is no partial recovery available: the
     * lost tasks held records that were in flight and state that was never snapshotted, so the
     * surviving tasks have already moved past any point the lost ones could be restarted from.
     */
    void onWorkerDead(String workerId) {
        quarantinedWorkers.add(workerId);
        executionGraphs.forEach((jobId, plan) -> {
            List<TaskAssignment> lost = plan.assignmentsFor(workerId);
            if (!lost.isEmpty()) {
                failJob(jobId, "worker " + workerId + " died holding " + lost.size()
                        + " task(s): " + lost.stream().map(TaskAssignment::key).toList());
            }
        });
    }

    private synchronized void restartFromCheckpoint(String jobId, CompletedCheckpoint checkpoint) {
        if (metadata.getJobState(jobId).orElse(JobState.FAILED) != JobState.RESTARTING) {
            return;
        }
        JobGraph graph = jobGraphs.get(jobId);
        if (graph == null) {
            metadata.putJobState(jobId, JobState.FAILED);
            log.error("job {} could not restart: graph is absent from this master", jobId);
            return;
        }
        try {
            List<RegisteredWorker> workers = schedulableWorkers();
            ExecutionGraph plan = ExecutionGraphCompiler.compile(graph, workers);
            Map<String, String> handles = new LinkedHashMap<>();
            checkpoint.taskStates().forEach((task, state) -> handles.put(task, state.stateHandleUri()));
            // Publish the generation we are about to deploy before making any remote call. If
            // deployment or completion replay fails (or this master dies between them), the
            // next recovery attempt can identify and cancel this exact assignment set rather
            // than accidentally targeting the previous generation.
            executionGraphs.put(jobId, plan);
            finishedTasks.put(jobId, ConcurrentHashMap.newKeySet());
            taskStatuses.put(jobId, new ConcurrentHashMap<>());
            Map<String, String> assignments = new LinkedHashMap<>();
            plan.assignments().forEach((key, assignment) ->
                    assignments.put(key, assignment.workerId()));
            metadata.putAssignments(jobId, assignments);
            // All tasks -- including healthy ones -- are redeployed. See the class-level comment:
            // mixing a restored task with survivors that kept running would replay an inconsistent
            // part of the stream.
            deployer.deployFromCheckpoint(graph, plan, workers, handles);
            // A commit acknowledgement can be lost when the master dies after a sink has
            // committed but before the sink clears its pending checkpoint state. Replay the
            // durable checkpoint after every restore; transactional sinks make this idempotent.
            deployer.notifySinks(graph, plan, checkpoint.checkpointId());
            metadata.putJobState(jobId, JobState.RUNNING);
            startCheckpointing(graph, plan);
            log.info("job {} restarted from checkpoint {}", jobId, checkpoint.checkpointId());
        } catch (RuntimeException failure) {
            // This is a failed restart attempt, not an opportunity to leave a half-deployed job
            // marked RUNNING. Reuse the normal state transition and fixed-delay policy.
            metadata.putJobState(jobId, JobState.RUNNING);
            failJob(jobId, "restart from checkpoint " + checkpoint.checkpointId()
                    + " failed: " + failure.getMessage());
        }
    }

    private List<RegisteredWorker> schedulableWorkers() {
        return metadata.listWorkers().stream()
                .filter(worker -> !quarantinedWorkers.contains(worker.workerId()))
                .toList();
    }

    private void startCheckpointing(JobGraph graph, ExecutionGraph plan) {
        checkpoints.start(graph.jobId(), checkpointTasks(plan), new CheckpointCoordinator.Actions() {
            @Override
            public void triggerSources(String jobId, long checkpointId, long triggerTimestamp) {
                deployer.triggerSources(graph, plan, checkpointId, triggerTimestamp);
            }

            @Override
            public void notifySinks(String jobId, long checkpointId) {
                try {
                    deployer.notifySinks(graph, plan, checkpointId);
                } catch (RuntimeException failure) {
                    // The checkpoint pointer is already durable when this callback runs. Do
                    // not admit a later checkpoint while an external commit is uncertain:
                    // roll the whole job back to this checkpoint and replay the idempotent
                    // completion notification as part of recovery.
                    failJob(jobId, "checkpoint " + checkpointId
                            + " completion delivery failed: " + failure.getMessage());
                    throw failure;
                }
            }

            @Override
            public void checkpointAborted(String jobId, long checkpointId, String reason) {
                log.warn("checkpoint {} for job {} aborted: {}", checkpointId, jobId, reason);
                deployer.abortCheckpoint(graph, plan, checkpointId);
            }
        });
    }

    private static Set<CheckpointCoordinator.TaskKey> checkpointTasks(ExecutionGraph plan) {
        java.util.LinkedHashSet<CheckpointCoordinator.TaskKey> tasks = new java.util.LinkedHashSet<>();
        plan.chainGroups().forEach(chain -> {
            for (int subtask = 0; subtask < chain.parallelism(); subtask++) {
                tasks.add(new CheckpointCoordinator.TaskKey(chain.head().id(), subtask));
            }
        });
        return Set.copyOf(tasks);
    }

    /**
     * Recovers what this master was doing, after a restart.
     *
     * <p>Reads back every job it knew about, so that a restarted master can distinguish a job it
     * should be watching from one that ended. Live graphs are reconciled from their latest
     * durable checkpoint as a whole. If workers have not advertised enough capacity yet, retain
     * the graph and assignments until registration or a durable lease change permits recovery.
     *
     * @return what was found, by job id
     */
    public synchronized Map<String, RecoveredJob> recover() {
        Map<String, RecoveredJob> recovered = new LinkedHashMap<>();
        List<RegisteredWorker> workers = metadata.listWorkers();
        deployer.observeWorkers(workers);
        for (String jobId : metadata.listJobs()) {
            JobState state = metadata.getJobState(jobId).orElse(JobState.CREATED);
            Map<String, String> assignments = metadata.getAssignments(jobId);
            Optional<byte[]> graph = metadata.getJobGraph(jobId);
            Optional<String> cause = metadata.getJobFailureCause(jobId);

            recovered.put(jobId, new RecoveredJob(jobId, state, assignments,
                    graph.map(bytes -> bytes.length).orElse(0), cause));
            graph.ifPresent(bytes -> restoreRecoveredPlan(jobId, bytes, assignments, workers, state));
            log.info("recovered job {} in state {} with {} assignments",
                    jobId, state, assignments.size());
        }
        return recovered;
    }

    /** Finishes a whole-job restart whose master crashed between durable state transitions. */
    private synchronized void resumeInterruptedRecovery(String jobId, JobState recoveredState) {
        JobGraph graph = jobGraphs.get(jobId);
        ExecutionGraph plan = executionGraphs.get(jobId);
        if (graph == null || plan == null) {
            log.warn("cannot resume {} job {} until its graph and plan can be reconstructed",
                    recoveredState, jobId);
            return;
        }
        if (recoveredState == JobState.FAILING) {
            deployer.cancelAll(jobId, plan);
            checkpoints.stop(jobId);
        }
        Optional<CompletedCheckpoint> checkpoint = metadata.getLatestCompletedCheckpoint(jobId);
        if (checkpoint.isEmpty()) {
            metadata.putJobState(jobId, JobState.FAILED);
            log.error("job {} cannot resume recovery: it has no completed checkpoint", jobId);
            return;
        }
        int nextAttempt = restartAttempts.merge(jobId, 1, Integer::sum);
        if (nextAttempt > restartStrategy.maxAttempts()) {
            metadata.putJobState(jobId, JobState.FAILED);
            return;
        }
        metadata.putJobState(jobId, JobState.RESTARTING);
        recoveryScheduler.schedule(() -> restartFromCheckpoint(jobId, checkpoint.get()),
                restartStrategy.delay().toMillis(), TimeUnit.MILLISECONDS);
    }

    private void restoreRecoveredPlan(String jobId,
                                      byte[] serializedGraph,
                                      Map<String, String> assignments,
                                      List<RegisteredWorker> workers,
                                      JobState state) {
        try {
            JobGraph graph = SerializationUtil.fromBytes(serializedGraph);
            jobGraphs.put(jobId, graph);
            restoreRecoveredPlan(jobId, graph, assignments, workers, state);
        } catch (RuntimeException failure) {
            log.warn("could not reconstruct in-memory plan for recovered job {}: {}", jobId,
                    failure.getMessage());
        }
    }

    private void restoreRecoveredPlan(String jobId,
                                      JobGraph graph,
                                      Map<String, String> assignments,
                                      List<RegisteredWorker> workers,
                                      JobState state) {
        try {
            if (!state.isTerminal()) {
                deferredRecoveries.put(jobId, new DeferredRecovery(graph, Map.copyOf(assignments)));
            }
            int availableSlots = workers.stream().mapToInt(RegisteredWorker::slots).sum();
            if (workers.isEmpty() || availableSlots < assignments.size()) {
                log.info("deferring recovery of job {}: waiting for {} task slots, currently {}",
                        jobId, assignments.size(), availableSlots);
                return;
            }
            ExecutionGraph freshlyCompiled = ExecutionGraphCompiler.compile(graph, workers);
            if (assignments.size() != freshlyCompiled.taskCount()) {
                log.warn("job {} has {} persisted assignments for {} task(s); not restoring "
                                + "its in-memory plan", jobId, assignments.size(),
                        freshlyCompiled.taskCount());
                return;
            }
            Map<String, TaskAssignment> restoredAssignments = new LinkedHashMap<>();
            for (String key : freshlyCompiled.assignments().keySet()) {
                String workerId = assignments.get(key);
                if (workerId == null) {
                    return;
                }
                int split = key.lastIndexOf(':');
                restoredAssignments.put(key, new TaskAssignment(key.substring(0, split),
                        Integer.parseInt(key.substring(split + 1)), workerId));
            }
            ExecutionGraph restored = new ExecutionGraph(jobId, freshlyCompiled.vertices(),
                    freshlyCompiled.chainGroups(), restoredAssignments);
            executionGraphs.put(jobId, restored);
            // Registration and recover() share this master's monitor. Remove the reservation
            // before scheduling any restore so another registration cannot spend its budget
            // or queue a second recovery of the same durable cut.
            deferredRecoveries.remove(jobId);
            deployer.observeWorkers(workers);
            if (state == JobState.RUNNING) {
                // A surviving task may have an in-flight barrier whose id was never persisted.
                // Starting a fresh coordinator against it could reuse that id and produce an
                // inconsistent external-sink snapshot. Reconcile a RUNNING job exactly like a
                // worker failure: cancel every survivor and restore one coherent cut. Recovery
                // also replays the durable completion notification before barriers resume.
                failJob(jobId, "master recovered a running job; reconciling every task from "
                        + "the latest completed checkpoint");
            } else if (state == JobState.FAILING || state == JobState.RESTARTING) {
                resumeInterruptedRecovery(jobId, state);
            }
        } catch (RuntimeException failure) {
            log.warn("could not reconstruct in-memory plan for recovered job {}: {}", jobId,
                    failure.getMessage());
        }
    }

    private record DeferredRecovery(JobGraph graph, Map<String, String> assignments) {
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
        checkpoints.close();
        recoveryScheduler.shutdownNow();
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
