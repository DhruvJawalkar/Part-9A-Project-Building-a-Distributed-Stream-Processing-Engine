package dev.dhruv.streaming.api.metrics;

/**
 * A monotonically increasing count.
 *
 * <p>Counters are read by scraping their difference over time, so an operator should never
 * reset one. Records processed, conversions emitted, late events dropped: all counters.
 */
public interface Counter {

    /**
     * Adds one.
     */
    void increment();

    /**
     * Adds an arbitrary non-negative amount.
     *
     * @param amount how much to add
     */
    void increment(long amount);

    /**
     * Returns the current total. Useful mostly in tests, where asserting on a metric is the
     * cleanest way to check that an operator did what it was supposed to.
     *
     * @return the running total
     */
    long count();
}
