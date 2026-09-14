package dev.dhruv.streaming.api.state;

import java.io.IOException;
import java.util.Optional;

/**
 * A single value held per key.
 *
 * <p>Every method here is implicitly scoped to whatever key the runtime made current before
 * calling the operator. Two different keys reading the "same" {@code ValueState} object see
 * two entirely different values. The operator never passes a key in, which is what allows the
 * same operator code to run unchanged whether the job has one subtask or forty.
 *
 * @param <T> the stored type
 */
public interface ValueState<T> {

    /**
     * Returns the value for the current key, or empty if nothing has been stored for it yet.
     *
     * <p>Empty rather than {@code null}: a fresh key and a key explicitly set to nothing are
     * the same situation here, and making the caller handle it is the point.
     *
     * @return the current value, if any
     * @throws IOException if the state backend cannot be read
     */
    Optional<T> value() throws IOException;

    /**
     * Stores a value for the current key, replacing anything already there.
     *
     * @param value the value to store; must not be null, use {@link #clear()} to remove
     * @throws IOException if the state backend cannot be written
     */
    void update(T value) throws IOException;

    /**
     * Removes this state for the current key.
     *
     * <p>Calling this is not an optimisation, it is a correctness requirement. Keyed state
     * that is never cleared is a leak that grows with the cardinality of the key space, and
     * in a clickstream the key space is "every member who ever browsed".
     */
    void clear();
}
