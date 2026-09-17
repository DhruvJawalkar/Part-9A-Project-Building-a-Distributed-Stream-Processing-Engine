package dev.dhruv.streaming.runtime.transport;

import dev.dhruv.streaming.api.StreamElement;

/**
 * One outgoing channel: everything this task sends to one particular downstream subtask.
 *
 * <p>Two implementations, and which one a channel gets is decided at deployment time by whether
 * the two subtasks landed on the same worker. {@link LocalSubpartition} hands the element
 * straight to the other task's input gate; {@link RemoteSubpartition} batches it and ships it
 * over gRPC within the credit it has been granted.
 *
 * <p>Keeping both behind one interface is what lets the scheduler place tasks wherever it likes
 * without the task code knowing or caring. It also makes the cost of the decision visible in one
 * place: the local path is a queue handoff, the remote path is serialization, a network write,
 * and flow-control accounting.
 */
public interface ResultSubpartition extends AutoCloseable {

    /**
     * Queues one element for this destination.
     *
     * @param element the element to send
     * @throws InterruptedException if the sending task is cancelled while waiting for room or
     *                              for credit. Blocking here is backpressure working.
     */
    void add(StreamElement element) throws InterruptedException;

    /**
     * Sends whatever has been queued but not yet shipped.
     *
     * <p>A no-op for the local path, which never holds anything back.
     *
     * @throws InterruptedException if the sending task is cancelled while waiting
     */
    void flush() throws InterruptedException;

    @Override
    void close();
}
