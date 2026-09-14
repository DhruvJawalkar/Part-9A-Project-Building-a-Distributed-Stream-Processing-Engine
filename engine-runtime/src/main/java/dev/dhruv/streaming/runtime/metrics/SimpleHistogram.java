package dev.dhruv.streaming.runtime.metrics;

import dev.dhruv.streaming.api.metrics.Histogram;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * A histogram that keeps count, total and maximum rather than a full distribution.
 *
 * <p>Deliberately not a real histogram. Reservoir sampling and t-digests are solved problems
 * and Phase 7 hands the job to Micrometer; until then, what the demos actually need from
 * checkpoint alignment time is the mean and the worst case, and three numbers that a reader can
 * verify at a glance beat an approximation they have to trust.
 */
public final class SimpleHistogram implements Histogram {

    private final LongAdder count = new LongAdder();
    private final LongAdder total = new LongAdder();
    private final AtomicLong max = new AtomicLong(Long.MIN_VALUE);

    @Override
    public void record(long value) {
        count.increment();
        total.add(value);
        max.accumulateAndGet(value, Math::max);
    }

    /**
     * Returns how many observations have been recorded.
     *
     * @return the observation count
     */
    public long count() {
        return count.sum();
    }

    /**
     * Returns the mean observation, or zero if there have been none.
     *
     * @return the mean
     */
    public double mean() {
        long observations = count.sum();
        return observations == 0 ? 0.0 : (double) total.sum() / observations;
    }

    /**
     * Returns the largest observation, or zero if there have been none.
     *
     * @return the maximum
     */
    public long max() {
        long observed = max.get();
        return observed == Long.MIN_VALUE ? 0L : observed;
    }
}
