package dev.dhruv.streaming.runtime;

import dev.dhruv.streaming.api.StreamElement;
import dev.dhruv.streaming.api.StreamRecord;

/**
 * Where a task sends what it produces.
 *
 * <p>Two methods, because the engine sends two fundamentally different things down the same
 * channel. A data record goes to <em>one</em> downstream subtask, chosen by the exchange
 * strategy. A control element -- a watermark or a checkpoint barrier -- goes to <em>every</em>
 * one, because it is a statement about the stream as a whole and a downstream task that never
 * received it would wait forever for something that already happened.
 *
 * <p>In Phase 1 the implementation is a handful of in-memory queues. From Phase 2 it is a gRPC
 * stream to another process. Nothing in {@code Task} changes when that swap happens, which is
 * the reason this interface exists at all.
 */
interface Output extends AutoCloseable {

    /**
     * Sends one record to whichever downstream subtask the exchange strategy selects.
     *
     * @param record the record to route
     * @throws InterruptedException if the task is cancelled while the downstream queue is full.
     *                              Blocking here is not a flaw: it is backpressure, propagating
     *                              one hop upstream exactly as it should.
     */
    void emit(StreamRecord<?> record) throws InterruptedException;

    /**
     * Sends one element to every downstream subtask.
     *
     * @param element the watermark or barrier to broadcast
     * @throws InterruptedException if the task is cancelled while a downstream queue is full
     */
    void broadcast(StreamElement element) throws InterruptedException;

    @Override
    void close();
}
