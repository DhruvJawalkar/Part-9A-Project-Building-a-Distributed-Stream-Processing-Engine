package dev.dhruv.streaming.master;

import dev.dhruv.streaming.api.graph.JobGraph;
import dev.dhruv.streaming.master.graph.ExecutionGraph;
import dev.dhruv.streaming.metadata.RegisteredWorker;

import java.util.List;

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
     * Sends every task of a compiled plan to the worker that owns it.
     *
     * @param graph   the logical graph, whose operators are serialized into the deployments
     * @param plan    the physical plan
     * @param workers the workers it was compiled against, for their addresses
     */
    void deploy(JobGraph graph, ExecutionGraph plan, List<RegisteredWorker> workers);

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
