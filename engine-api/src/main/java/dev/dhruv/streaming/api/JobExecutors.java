package dev.dhruv.streaming.api;

import dev.dhruv.streaming.api.graph.JobGraph;

import java.util.ServiceLoader;

/**
 * Entry point for running a job.
 *
 * <p>Phase 1 offers one way to run one: locally, in the calling JVM. Phase 2 adds submission to
 * a master, at which point a job's {@code main()} chooses between running on this machine and
 * running on a cluster by changing this one line.
 */
public final class JobExecutors {

    private JobExecutors() {
    }

    /**
     * Runs a job in this JVM, one thread per subtask.
     *
     * @param graph the validated job graph
     * @return an executor that has not yet been started
     * @throws IllegalStateException if no engine implementation is on the classpath, which in
     *                               practice means the runtime module is missing from the
     *                               launch classpath
     */
    public static JobExecutor local(JobGraph graph) {
        return ServiceLoader.load(JobExecutorProvider.class).findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "no JobExecutorProvider on the classpath; the engine runtime is"
                                + " needed at run time even though a job compiles without it"))
                .create(graph);
    }
}
