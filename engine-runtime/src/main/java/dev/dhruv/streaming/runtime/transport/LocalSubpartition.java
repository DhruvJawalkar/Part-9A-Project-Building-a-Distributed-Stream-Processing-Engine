package dev.dhruv.streaming.runtime.transport;

import dev.dhruv.streaming.api.StreamElement;

/**
 * An outgoing channel to a task on this same worker.
 *
 * <p>The element is put straight into the destination's input gate. No serialization, no
 * network, no credit accounting -- the gate's bounded queue already blocks the sender when the
 * receiver falls behind, which is the whole of what flow control needs to do when both ends
 * share a process.
 *
 * <p>Worth comparing against {@link RemoteSubpartition} directly. Everything that class does is
 * work this one does not have to: turning objects into bytes, batching to amortise a network
 * write, and rebuilding by hand the backpressure that a queue provides for free. That gap is
 * why schedulers try to co-locate communicating tasks, and why operator chaining -- which
 * removes even this handoff -- is worth as much as it is.
 *
 * @param channelIndex which of the destination's input channels this sender is
 * @param destination  the receiving task's input gate
 */
record LocalSubpartition(int channelIndex, InputGate destination) implements ResultSubpartition {

    @Override
    public void add(StreamElement element) throws InterruptedException {
        destination.enqueue(channelIndex, element);
    }

    @Override
    public void flush() {
        // Nothing is ever held back, so there is nothing to flush.
    }

    @Override
    public void close() {
        // The gate belongs to the receiving task, which closes it.
    }
}
