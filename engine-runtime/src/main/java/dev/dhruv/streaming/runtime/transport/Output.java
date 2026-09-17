package dev.dhruv.streaming.runtime.transport;

import dev.dhruv.streaming.api.StreamElement;
import dev.dhruv.streaming.api.StreamRecord;

/**
 * Where a task sends what it produces.
 *
 * <p>Two methods, because the engine sends two fundamentally different things down the same
 * channel. A data record goes to <em>one</em> downstream subtask, chosen by the exchange
 * strategy. A control element -- a watermark or a checkpoint barrier -- goes to <em>every</em>
 * subtask this task feeds, because it is a statement about the stream as a whole, and a
 * downstream task that never received it would wait forever for something that already
 * happened.
 *
 * <p>The implementation behind this interface changed completely between Phase 1 and Phase 2:
 * in-memory queues became gRPC streams with flow control. Nothing in {@code OperatorTask}
 * changed at all, which is the only real test of whether the boundary was drawn in the right
 * place.
 */
public interface Output extends AutoCloseable {

    /**
     * Sends one record to whichever downstream subtask the exchange strategy selects.
     *
     * @param record the record to route
     * @throws InterruptedException if the task is cancelled while waiting for room downstream.
     *                              Blocking here is not a flaw: it is backpressure, propagating
     *                              one hop upstream exactly as it should.
     */
    void emit(StreamRecord<?> record) throws InterruptedException;

    /**
     * Sends one element to every downstream subtask this task feeds.
     *
     * @param element the watermark or barrier to broadcast
     * @throws InterruptedException if the task is cancelled while waiting
     */
    void broadcast(StreamElement element) throws InterruptedException;

    /**
     * Ships anything batched but not yet sent.
     *
     * <p>Called on a timer, and again before a task stops. Without the timer, a trickle of
     * records could sit in a half-full buffer indefinitely; without the call at shutdown, the
     * last buffer of a finished job would never arrive.
     *
     * @throws InterruptedException if the task is cancelled while waiting
     */
    void flush() throws InterruptedException;

    @Override
    void close();
}
