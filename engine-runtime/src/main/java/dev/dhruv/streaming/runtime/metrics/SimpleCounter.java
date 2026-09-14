package dev.dhruv.streaming.runtime.metrics;

import dev.dhruv.streaming.api.metrics.Counter;

import java.util.concurrent.atomic.LongAdder;

/**
 * A counter backed by a {@link LongAdder}.
 *
 * <p>Written by exactly one task thread and read by whichever thread is scraping metrics, which
 * is the one case where a task's single-threaded guarantee does not extend to its data. A
 * {@code LongAdder} rather than a lock because the write is on the per-record path and the read
 * is not.
 */
public final class SimpleCounter implements Counter {

    private final LongAdder value = new LongAdder();

    @Override
    public void increment() {
        value.increment();
    }

    @Override
    public void increment(long amount) {
        value.add(amount);
    }

    @Override
    public long count() {
        return value.sum();
    }
}
