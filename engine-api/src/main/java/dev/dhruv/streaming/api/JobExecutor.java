package dev.dhruv.streaming.api;

/**
 * A running job.
 *
 * <p>Exists so that a job's {@code main()} can start what it just described without being able
 * to see the engine that runs it. That separation is not ceremony: it is the check that the
 * API is complete. If running a job required reaching into the runtime, the runtime would be
 * part of the user-facing surface whether or not anyone admitted it.
 */
public interface JobExecutor extends AutoCloseable {

    /**
     * Builds every task and starts it. Returns once the job is running, not once it is done.
     */
    void start();

    /**
     * Blocks until every task has stopped.
     *
     * <p>For a job reading an unbounded source, that is until something cancels it.
     *
     * @throws InterruptedException if the waiting thread is interrupted
     */
    void awaitTermination() throws InterruptedException;

    /**
     * Stops the job and releases its resources.
     */
    @Override
    void close();
}
