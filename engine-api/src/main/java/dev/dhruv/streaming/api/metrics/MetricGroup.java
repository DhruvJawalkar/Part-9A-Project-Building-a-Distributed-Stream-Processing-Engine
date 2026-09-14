package dev.dhruv.streaming.api.metrics;

import java.util.function.LongSupplier;

/**
 * The metrics an operator can register, already scoped to the task that owns it.
 *
 * <p>An operator asks for a metric by bare name. The runtime attaches the job, operator and
 * subtask index, so a name registered here as {@code "buffered-clicks"} surfaces per subtask
 * rather than summed across them. That distinction is the whole reason the hot-key demo is
 * visible at all: an aggregate would show a perfectly healthy job.
 */
public interface MetricGroup {

    /**
     * Returns the named counter, creating it on first request.
     *
     * @param name metric name, scoped to this subtask
     * @return the counter
     */
    Counter counter(String name);

    /**
     * Returns the named histogram, creating it on first request.
     *
     * @param name metric name, scoped to this subtask
     * @return the histogram
     */
    Histogram histogram(String name);

    /**
     * Registers a value to be sampled when metrics are collected.
     *
     * <p>The supplier is invoked on the task thread, so it must be cheap and must not block.
     * Reporting the size of a buffer is the intended use; computing one is not.
     *
     * @param name  metric name, scoped to this subtask
     * @param value supplier sampled at collection time
     */
    void gauge(String name, LongSupplier value);
}
