package dev.dhruv.streaming.runtime.transport;

import dev.dhruv.streaming.api.StreamElement;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntConsumer;

/**
 * Everything arriving at one task, kept one queue per input channel.
 *
 * <h2>Why per channel, and not one queue</h2>
 *
 * <p>A single queue would be simpler and would work for Phase 2, where a task consumes whatever
 * turns up. It is the wrong shape for what comes next, and the reason is worth stating now
 * rather than discovering later.
 *
 * <p>A watermark is a claim about <em>one</em> channel, so a task has to remember what each of
 * its inputs last told it and take the minimum (Phase 3). A checkpoint barrier means "everything
 * before me on <em>this</em> channel belongs to the checkpoint", so a task has to stop consuming
 * that one channel while continuing with the others until the rest catch up (Phase 4). Both
 * mechanisms need the identity of the channel an element came from, and alignment additionally
 * needs to pause a channel without pausing the task. Neither is expressible once the elements
 * have been merged into one queue.
 *
 * <p>So the gate keeps a bounded queue per channel and hands out the channel index along with
 * the element. Phase 2 does not use either fact yet. It costs nothing to have them.
 *
 * <h2>Backpressure and credit</h2>
 *
 * <p>Each channel's queue is bounded, so a producer filling one waits. For a producer in this
 * JVM that is the whole mechanism -- exactly as in Phase 1.
 *
 * <p>For a producer in another process, waiting is not something the network will do on the
 * sender's behalf, so the gate reports space as it frees up through a credit listener, and the
 * sender ships only what it has been granted. Same idea, made explicit because a socket will not
 * do it for you.
 */
public final class InputGate {

    /**
     * How many elements may sit in one channel's queue.
     *
     * <p>Also the credit a remote sender can hold for that channel, which is why the number
     * matters twice: it bounds memory here, and it bounds how much is in flight there.
     */
    public static final int DEFAULT_CHANNEL_CAPACITY = 512;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final Condition notFull = lock.newCondition();

    private final List<Deque<StreamElement>> queues;
    private final BitSet blockedChannels;
    private final int capacityPerChannel;
    private final IntConsumer onChannelDrained;

    /** Round-robin cursor, so one busy channel cannot starve the others. */
    private int nextChannel;

    /**
     * Creates a gate.
     *
     * @param channelCount       how many upstream subtasks feed this task
     * @param capacityPerChannel how many elements one channel may hold
     * @param onChannelDrained   called with a channel index when an element is consumed from it,
     *                           so a remote sender can be granted more credit. Pass a no-op for
     *                           a task whose inputs are all local.
     */
    public InputGate(int channelCount, int capacityPerChannel, IntConsumer onChannelDrained) {
        this.queues = new ArrayList<>(channelCount);
        for (int i = 0; i < channelCount; i++) {
            queues.add(new ArrayDeque<>());
        }
        this.blockedChannels = new BitSet(channelCount);
        this.capacityPerChannel = capacityPerChannel;
        this.onChannelDrained = onChannelDrained;
    }

    /**
     * Creates a gate with default capacity and no credit reporting.
     *
     * @param channelCount how many upstream subtasks feed this task
     */
    public InputGate(int channelCount) {
        this(channelCount, DEFAULT_CHANNEL_CAPACITY, channel -> {
        });
    }

    /**
     * Returns how many channels feed this gate.
     *
     * @return the channel count
     */
    public int channelCount() {
        return queues.size();
    }

    /**
     * Adds an element to one channel, waiting if that channel is full.
     *
     * <p>Waiting here is the point, not a limitation: it is what makes a slow consumer slow its
     * producer down rather than accumulate an unbounded backlog.
     *
     * @param channelIndex which input this arrived on
     * @param element      the element
     * @throws InterruptedException if the producing thread is interrupted while waiting
     */
    public void enqueue(int channelIndex, StreamElement element) throws InterruptedException {
        lock.lockInterruptibly();
        try {
            Deque<StreamElement> queue = queues.get(channelIndex);
            while (queue.size() >= capacityPerChannel) {
                notFull.await();
            }
            queue.addLast(element);
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Takes the next element from any unblocked channel.
     *
     * <p>Channels are visited round-robin rather than in order, so a channel that always has
     * data cannot starve one that sometimes does. That fairness is not cosmetic: a starved
     * channel's watermark never advances, which in Phase 3 would hold back the task's whole
     * event-time clock.
     *
     * @param timeout how long to wait for something to arrive
     * @param unit    the timeout's unit
     * @return the element and the channel it came from, or empty if nothing arrived in time
     * @throws InterruptedException if the consuming thread is interrupted while waiting
     */
    public Optional<IncomingElement> poll(long timeout, TimeUnit unit)
            throws InterruptedException {

        long remainingNanos = unit.toNanos(timeout);
        lock.lockInterruptibly();
        try {
            while (true) {
                Optional<IncomingElement> taken = pollUnblockedChannels();
                if (taken.isPresent()) {
                    return taken;
                }
                if (remainingNanos <= 0) {
                    return Optional.empty();
                }
                remainingNanos = notEmpty.awaitNanos(remainingNanos);
            }
        } finally {
            lock.unlock();
        }
    }

    /** Caller holds the lock. */
    private Optional<IncomingElement> pollUnblockedChannels() {
        for (int attempt = 0; attempt < queues.size(); attempt++) {
            int channel = nextChannel;
            nextChannel = (nextChannel + 1) % queues.size();

            if (blockedChannels.get(channel)) {
                continue;
            }
            Deque<StreamElement> queue = queues.get(channel);
            if (queue.isEmpty()) {
                continue;
            }

            StreamElement element = queue.removeFirst();
            notFull.signalAll();
            // Outside the caller's view but inside the lock: cheap, and it keeps the credit
            // report ordered with the dequeue it reports.
            onChannelDrained.accept(channel);
            return Optional.of(new IncomingElement(element, channel));
        }
        return Optional.empty();
    }

    /**
     * Stops consuming one channel, leaving its arrivals to accumulate.
     *
     * <p>Phase 4 uses this and nothing else does. When a barrier arrives on a channel, that
     * channel has said everything it has to say about the checkpoint; blocking it while the
     * other channels catch up is what makes the resulting snapshot consistent. The records that
     * pile up behind the block are not lost, they are simply after the barrier, which is
     * precisely where they belong.
     *
     * @param channelIndex the channel to stop consuming
     */
    public void blockChannel(int channelIndex) {
        lock.lock();
        try {
            blockedChannels.set(channelIndex);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Resumes every blocked channel. Called after a snapshot completes.
     */
    public void unblockAllChannels() {
        lock.lock();
        try {
            blockedChannels.clear();
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns how many channels are currently blocked. Phase 4 asserts on this.
     *
     * @return the blocked channel count
     */
    public int blockedChannelCount() {
        lock.lock();
        try {
            return blockedChannels.cardinality();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns how many elements are queued across all channels, for metrics.
     *
     * @return the total queued
     */
    public int queuedElements() {
        lock.lock();
        try {
            return queues.stream().mapToInt(Deque::size).sum();
        } finally {
            lock.unlock();
        }
    }

    /** Returns the bounded capacity across all input channels, for observable backpressure. */
    public int totalCapacity() {
        return channelCount() * capacityPerChannel;
    }

    /** One saturated channel blocks its own producer even if other queues are empty. */
    public boolean hasFullChannel() {
        lock.lock();
        try {
            return queues.stream().anyMatch(queue -> queue.size() >= capacityPerChannel);
        } finally {
            lock.unlock();
        }
    }

    /**
     * An element together with the channel it arrived on.
     *
     * @param element      what arrived
     * @param channelIndex which input it came from
     */
    public record IncomingElement(StreamElement element, int channelIndex) {
    }
}
