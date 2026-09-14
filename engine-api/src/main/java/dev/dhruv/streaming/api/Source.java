package dev.dhruv.streaming.api;

import java.io.Serializable;

/**
 * Where records enter the job.
 *
 * <p>Sources are pull-based rather than push-based: the runtime asks for records when it is
 * ready for them, rather than the source delivering them whenever it likes. That single choice
 * is what makes backpressure work at all. A source that pushes has nowhere to put records when
 * the job downstream slows down, so it must either buffer without bound or drop; a source that
 * is polled simply stops being polled, and the slowdown propagates all the way back to the
 * broker with no mechanism required.
 *
 * <p>Timestamps are not this interface's concern. A source emits raw values, and the runtime
 * stamps them using the {@link TimestampAssigner} configured on the graph. Keeping the two
 * apart means the same source can feed an event-time job and a processing-time one.
 *
 * @param <T> the type of record produced
 */
public interface Source<T> extends Serializable {

    /**
     * Called once before the first poll, on the task thread that will do the polling.
     *
     * <p>This is where connections are opened. Doing it here rather than in a constructor is
     * what lets a source be serialized and shipped to a worker: the object that crosses the
     * wire carries configuration only, and acquires resources once it arrives.
     *
     * @param context what this subtask should read
     * @throws Exception any failure; the task fails to start
     */
    void open(SourceContext context) throws Exception;

    /**
     * Fetches whatever records are available right now and emits them.
     *
     * <p>Expected to block briefly when there is nothing to read, rather than spinning or
     * blocking indefinitely. The runtime calls this in a loop and needs to regain control
     * periodically to check for cancellation and to emit watermarks on schedule.
     *
     * @param out where to emit the records read
     * @return true if the source may produce more, false once it is permanently exhausted.
     *         An unbounded source always returns true; a file replay returns false at EOF,
     *         which is what lets the runtime emit {@link Watermark#MAX} and close every
     *         still-open window rather than leaving the job hanging.
     * @throws Exception any failure; the runtime fails the task
     */
    boolean poll(Collector<T> out) throws Exception;

    /**
     * Releases resources. Called once, even if {@link #poll} threw.
     *
     * @throws Exception any failure; logged, but the task still stops
     */
    void close() throws Exception;
}
