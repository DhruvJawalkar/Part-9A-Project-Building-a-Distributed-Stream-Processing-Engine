package dev.dhruv.streaming.api.state;

import java.io.IOException;
import java.util.List;

/**
 * A list of values held per key, appended to over time.
 *
 * <p>Scoped to the current key exactly like {@link ValueState}. Used where an operator
 * genuinely must retain the individual elements rather than fold them into an accumulator.
 * The interval join buffers both of its sides this way, because it cannot know in advance
 * which buffered element a future arrival will match.
 *
 * <p>Where an accumulator would do, prefer {@link ValueState} holding it. The difference
 * between an incremental aggregate and a buffered list is the difference between state that
 * stays flat and state that grows with traffic.
 *
 * @param <T> the element type
 */
public interface ListState<T> {

    /**
     * Returns the elements stored for the current key, in insertion order. Empty if none.
     *
     * @return the current elements
     * @throws IOException if the state backend cannot be read
     */
    Iterable<T> get() throws IOException;

    /**
     * Appends one element for the current key.
     *
     * @param value the element to append
     * @throws IOException if the state backend cannot be written
     */
    void add(T value) throws IOException;

    /**
     * Replaces the whole list for the current key.
     *
     * <p>This is how eviction is expressed: read the list, drop what the watermark says can
     * no longer match, write back what remains.
     *
     * @param values the replacement contents
     * @throws IOException if the state backend cannot be written
     */
    void update(List<T> values) throws IOException;

    /**
     * Removes this state for the current key.
     */
    void clear();
}
