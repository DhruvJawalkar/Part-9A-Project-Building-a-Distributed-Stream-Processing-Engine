package dev.dhruv.streaming.api;

import dev.dhruv.streaming.api.graph.JobGraph;

/**
 * Supplies a {@link JobExecutor}, discovered at runtime through {@link java.util.ServiceLoader}.
 *
 * <p>The one piece of indirection in the engine, and it buys something specific: it is what
 * lets {@code lms-job} compile against the API alone while still being runnable. The job
 * module names this interface; the implementation arrives on the classpath at launch.
 *
 * <p>Plain JDK service loading, deliberately. No dependency injection container, no annotation
 * processing, no classpath scanning -- the binding is a single line in a
 * {@code META-INF/services} file, which is greppable and which a reader can follow in one hop.
 */
public interface JobExecutorProvider {

    /**
     * Creates an executor for a graph.
     *
     * @param graph the validated job graph
     * @return an executor that has not yet been started
     */
    JobExecutor create(JobGraph graph);
}
