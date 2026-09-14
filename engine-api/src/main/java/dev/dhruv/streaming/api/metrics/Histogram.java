package dev.dhruv.streaming.api.metrics;

/**
 * A distribution of observed values.
 *
 * <p>Used where an average would lie. Checkpoint alignment time is the motivating case: the
 * mean alignment across a hundred checkpoints tells you nothing useful, while the fact that
 * the worst one took four seconds tells you a great deal.
 */
public interface Histogram {

    /**
     * Records one observation.
     *
     * @param value the observed value, in whatever unit the metric is named for
     */
    void record(long value);
}
