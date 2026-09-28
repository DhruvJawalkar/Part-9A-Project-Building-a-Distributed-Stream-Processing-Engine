package dev.dhruv.streaming.master;

import dev.dhruv.streaming.api.graph.JobGraph;
import dev.dhruv.streaming.master.graph.ExecutionGraph;
import dev.dhruv.streaming.metadata.RegisteredWorker;

import java.util.List;
import java.util.Map;

/**
 * How the master actually gets tasks onto workers.
 *
 * <p>An interface so that the job state machine can be tested without a cluster. The real
 * implementation opens a gRPC channel per worker and calls {@code DeployTask}; the test
 * implementation records what it was asked to do. Both are exercised -- what changes between
 * them is whether a deployment crosses a process boundary, not whether the master's logic runs.
 */
public interface TaskDeployer {

    /**
     * Refreshes the deployer's address book after a master restart.
     *
     * <p>No task is deployed by this call. It only lets a recovered coordinator address the
     * workers that already host a still-running job.
     */
    default void observeWorkers(List<RegisteredWorker> workers) {
    }

    /**
     * Sends every task of a compiled plan to the worker that owns it.
     *
     * @param graph   the logical graph, whose operators are serialized into the deployments
     * @param plan    the physical plan
     * @param workers the workers it was compiled against, for their addresses
     */
    void deploy(JobGraph graph, ExecutionGraph plan, List<RegisteredWorker> workers);

    /**
     * Redeploys every vertical slice from one completed checkpoint.
     *
     * <p>The default keeps old test deployers small. A real deployer overrides it and places
     * each opaque state handle in the corresponding {@code TaskDeployment} before the task is
     * allowed to process anything.
     *
     * @param stateHandles {@code operatorId:subtaskIndex} to opaque state-backend URI
     */
    default void deployFromCheckpoint(JobGraph graph,
                                      ExecutionGraph plan,
                                      List<RegisteredWorker> workers,
                                      Map<String, String> stateHandles) {
        deploy(graph, plan, workers);
    }

    /** Injects a barrier at source tasks only; all other tasks receive it through their input. */
    default void triggerSources(JobGraph graph, ExecutionGraph plan,
                                long checkpointId, long triggerTimestamp) {
        throw new UnsupportedOperationException("checkpoint triggering is not configured");
    }

    /** Announces a completed checkpoint to sink tasks only. */
    default void notifySinks(JobGraph graph, ExecutionGraph plan, long checkpointId) {
        // Useful for in-memory deployers in lifecycle tests. The production gRPC deployer
        // overrides this; a deployer with no external transactional sink has nothing to do.
    }

    /** Releases task inputs blocked while aligning an incomplete checkpoint. */
    default void abortCheckpoint(JobGraph graph, ExecutionGraph plan, long checkpointId) {
    }

    /**
     * Cancels every task of a job.
     *
     * <p>Best-effort by nature: this is called when something has already gone wrong, and some
     * of the workers being asked may be the ones that are not answering. A cancel that fails is
     * logged rather than thrown, because the job is being torn down regardless.
     *
     * @param jobId the job
     * @param plan  its plan, naming the tasks to cancel
     */
    void cancelAll(String jobId, ExecutionGraph plan);
}
