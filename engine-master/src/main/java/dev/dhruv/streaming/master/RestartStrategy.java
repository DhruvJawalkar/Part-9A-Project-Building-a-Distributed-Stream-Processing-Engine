package dev.dhruv.streaming.master;

import java.time.Duration;
import java.util.Objects;

/**
 * The intentionally small recovery policy used by this teaching engine.
 *
 * <p>A fixed delay makes the sequence observable: fail, cancel the whole job, wait, then deploy
 * every task from the same checkpoint. Production systems grow richer policies, but those do
 * not change the invariant this class protects -- an attempt count is job-scoped, not
 * task-scoped, because recovery is a coordinated rewind.
 */
public record RestartStrategy(int maxAttempts, Duration delay) {

    public RestartStrategy {
        Objects.requireNonNull(delay, "delay");
        if (maxAttempts < 0) {
            throw new IllegalArgumentException("maxAttempts must not be negative");
        }
        if (delay.isNegative()) {
            throw new IllegalArgumentException("restart delay must not be negative");
        }
    }

    /** Returns the clear default: three recovery attempts, one second apart. */
    public static RestartStrategy fixedDelayDefault() {
        return new RestartStrategy(3, Duration.ofSeconds(1));
    }
}
