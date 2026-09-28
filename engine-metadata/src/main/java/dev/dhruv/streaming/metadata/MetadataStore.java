package dev.dhruv.streaming.metadata;


import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Durable home for everything the master would otherwise lose when it dies.
 *
 * <h2>What belongs here, and what does not</h2>
 *
 * <p>The distinction is whether losing it would make a restarted master wrong rather than
 * merely uninformed. The job graph, the assignments, the current state and (from Phase 4) the
 * latest checkpoint pointer all belong: without them, a master that comes back has no way to
 * tell a job that is still running from one that never started, and would happily schedule a
 * second copy of tasks already processing.
 *
 * <p>Liveness does not belong. Which workers are up right now is not a fact worth persisting,
 * because it is only true for as long as it takes to read it. Workers register under a lease
 * they must keep renewing, so the store forgets them on its own when they stop; the master
 * learns its cluster by watching that prefix rather than by remembering it.
 *
 * <h2>Key layout</h2>
 *
 * <pre>
 *   /jobs/{jobId}/graph               serialized JobGraph, written once at submit
 *   /jobs/{jobId}/state               CREATED | RUNNING | FAILING | FAILED | FINISHED
 *   /jobs/{jobId}/cause               why it failed, written before the state that names it
 *   /jobs/{jobId}/assignments         chainGroupId:subtask -&gt; workerId
 *   /jobs/{jobId}/checkpoints/latest  checkpointId + per-task handles (Phase 4)
 *   /workers/{workerId}               host:port and slots, under a TTL lease
 * </pre>
 *
 * <p>An interface rather than a class so that the tests that matter can run without a container
 * around them. The etcd implementation is the real one; the in-memory implementation exists so
 * that a test about the job state machine is about the job state machine.
 */
public interface MetadataStore extends AutoCloseable {

    /**
     * Records a job's graph. Written once, at submission, before anything is scheduled.
     *
     * @param jobId           the job
     * @param serializedGraph the graph, serialized
     */
    void putJobGraph(String jobId, byte[] serializedGraph);

    /**
     * Reads back a job's graph.
     *
     * @param jobId the job
     * @return the serialized graph, if this job is known
     */
    Optional<byte[]> getJobGraph(String jobId);

    /**
     * Records a job's state.
     *
     * <p>Call this <em>before</em> acting on the transition, never after. The window between
     * deciding and recording is the window in which a master crash loses track of what it was
     * doing.
     *
     * @param jobId the job
     * @param state its new state
     */
    void putJobState(String jobId, JobState state);

    /**
     * Reads a job's state.
     *
     * @param jobId the job
     * @return its state, if this job is known
     */
    Optional<JobState> getJobState(String jobId);

    /**
     * Records why a job is failing.
     *
     * <p>Written before the {@code FAILING} state that it explains, so that a cause is never
     * missing for a state that implies one.
     *
     * @param jobId the job
     * @param cause human-readable explanation
     */
    void putJobFailureCause(String jobId, String cause);

    /**
     * Reads why a job failed.
     *
     * @param jobId the job
     * @return the cause, if one was recorded
     */
    Optional<String> getJobFailureCause(String jobId);

    /**
     * Records where each vertical slice was scheduled.
     *
     * @param jobId       the job
     * @param assignments {@code chainGroupId:subtaskIndex} to worker id
     */
    void putAssignments(String jobId, Map<String, String> assignments);

    /**
     * Reads back where each slice was scheduled.
     *
     * @param jobId the job
     * @return the assignments, empty if none were recorded
     */
    Map<String, String> getAssignments(String jobId);

    /**
     * Atomically replaces the job's recovery pointer with a fully completed checkpoint.
     *
     * <p>There is intentionally no operation for partial acknowledgements here. They belong to
     * the coordinator's short-lived in-memory bookkeeping; making them durable would make it too
     * easy for recovery to mistake an incomplete cut for a valid one.
     *
     * @param jobId job that owns the checkpoint
     * @param checkpoint latest checkpoint that every task acknowledged
     */
    void putLatestCompletedCheckpoint(String jobId, CompletedCheckpoint checkpoint);

    /**
     * Returns the most recent job-wide checkpoint, if the job has ever completed one.
     *
     * @param jobId job to inspect
     * @return the complete recovery point, never a partial checkpoint
     */
    Optional<CompletedCheckpoint> getLatestCompletedCheckpoint(String jobId);

    /**
     * Lists the jobs this store knows about.
     *
     * @return job ids
     */
    List<String> listJobs();

    /**
     * Registers a worker under a lease that must be kept alive.
     *
     * <p>The lease is what makes worker liveness self-cleaning. A worker that stops renewing --
     * because it crashed, or was killed, or lost the network -- disappears from the store on its
     * own after the TTL, with no one needing to notice and delete it. A registry that required
     * an explicit deregistration would be wrong precisely in the case that matters, since a
     * process killed with {@code -9} never gets to say goodbye.
     *
     * @param worker     the worker's address and capacity
     * @param ttlSeconds how long the registration survives without renewal
     * @return a handle that renews the lease until closed
     */
    WorkerRegistration registerWorker(RegisteredWorker worker, long ttlSeconds);

    /**
     * Lists the workers currently registered.
     *
     * @return the live workers, ordered by id so that scheduling is reproducible
     */
    List<RegisteredWorker> listWorkers();

    /**
     * Watches for workers appearing and disappearing.
     *
     * <p>A watch rather than a poll. The master needs to know about a new worker so it can
     * schedule onto it, and about a lost one so it can fail the job; polling would add latency
     * to both for no benefit, and the store already knows the moment either happens.
     *
     * @param onChange called with the new worker list whenever it changes
     * @return a handle that stops watching when closed
     */
    AutoCloseable watchWorkers(Consumer<List<RegisteredWorker>> onChange);

    @Override
    void close();

    /**
     * A worker's registration, kept alive for as long as it is held.
     */
    interface WorkerRegistration extends AutoCloseable {

        /**
         * Stops renewing, and so removes the worker from the store.
         */
        @Override
        void close();
    }
}
