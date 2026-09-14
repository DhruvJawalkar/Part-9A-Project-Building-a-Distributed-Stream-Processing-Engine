package dev.dhruv.streaming.runtime;

import dev.dhruv.streaming.api.JobExecutor;
import dev.dhruv.streaming.api.JobExecutorProvider;
import dev.dhruv.streaming.api.graph.JobGraph;

/**
 * Binds {@link dev.dhruv.streaming.api.JobExecutors#local} to {@link LocalJobExecutor}.
 *
 * <p>Registered in {@code META-INF/services/dev.dhruv.streaming.api.JobExecutorProvider}. That
 * file is the entire binding, and it is the reason a job module can compile without ever seeing
 * this class.
 */
public final class LocalJobExecutorProvider implements JobExecutorProvider {

    @Override
    public JobExecutor create(JobGraph graph) {
        return new LocalJobExecutor(graph);
    }
}
