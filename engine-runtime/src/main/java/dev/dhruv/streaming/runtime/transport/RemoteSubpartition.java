package dev.dhruv.streaming.runtime.transport;

import com.google.protobuf.ByteString;
import dev.dhruv.streaming.api.StreamElement;
import dev.dhruv.streaming.rpc.DataBuffer;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * An outgoing channel to a task on another worker, with credit-based flow control.
 *
 * <h2>Why credit, when TCP already has backpressure</h2>
 *
 * <p>The obvious objection to this class is that it rebuilds something the network already does.
 * TCP does apply backpressure: fill the receiver's window and the sender's writes block. So why
 * account for credit by hand?
 *
 * <p>Because one TCP connection carries several logical channels. A worker sending to four
 * downstream subtasks on the same peer multiplexes them over one connection, and TCP has no idea
 * that those are four independent streams. If one receiving subtask is slow, its unread bytes
 * fill the shared connection's window and the sender stops -- for <em>all four</em>. Three
 * healthy subtasks stall behind one slow sibling, and nothing in any log explains why.
 *
 * <p>Credit fixes that by moving the decision to the only place that can make it correctly. Each
 * receiver announces how many buffers it can take, per channel, and the sender ships only within
 * that. A slow subtask stops granting credit and its own sender stops; the others are unaffected
 * because their credit is separate.
 *
 * <p>There is a second benefit that matters from Phase 4. Because the sender never writes more
 * than the receiver has room for, data does not pile up in socket buffers, so a checkpoint
 * barrier is never stuck behind megabytes of records that some intermediate buffer already
 * accepted. Alignment time is bounded by what the engine can see rather than by what the kernel
 * happens to be holding.
 *
 * <h2>Batching, and the dial it exposes</h2>
 *
 * <p>Elements accumulate into a buffer that is shipped when it is full or when
 * {@code bufferTimeoutMs} expires, whichever comes first. The timeout is the project's
 * latency-versus-throughput dial: at zero, every record is its own network write and latency is
 * as low as it goes; raised, records share a write and throughput climbs while each record waits
 * a little longer for company.
 */
final class RemoteSubpartition implements ResultSubpartition {

    private static final Logger log = LoggerFactory.getLogger(RemoteSubpartition.class);

    /** How long to wait for credit before giving up and failing the task. */
    private static final long CREDIT_TIMEOUT_SECONDS = 120;

    private final String senderTaskId;
    private final String targetOperatorId;
    private final int targetSubtask;
    private final StreamObserver<DataBuffer> outbound;
    private final Semaphore credit;
    private final int maxBufferElements;

    private final List<StreamElement> pending = new ArrayList<>();
    private volatile boolean closed;

    RemoteSubpartition(String senderTaskId,
                       String targetOperatorId,
                       int targetSubtask,
                       StreamObserver<DataBuffer> outbound,
                       int maxBufferElements) {
        this.senderTaskId = senderTaskId;
        this.targetOperatorId = targetOperatorId;
        this.targetSubtask = targetSubtask;
        this.outbound = outbound;
        this.maxBufferElements = maxBufferElements;
        // Starts at zero. The sender may ship nothing until the receiver has said how much it
        // can take, which it does as soon as the stream opens. Starting at a non-zero guess
        // would be the one thing this mechanism exists to prevent.
        this.credit = new Semaphore(0);
    }

    /**
     * Releases credit granted by the receiver.
     *
     * @param buffers how many more buffers the receiver will accept
     */
    void grantCredit(int buffers) {
        credit.release(buffers);
    }

    @Override
    public synchronized void add(StreamElement element) throws InterruptedException {
        pending.add(element);
        if (pending.size() >= maxBufferElements) {
            ship();
        }
    }

    @Override
    public synchronized void flush() throws InterruptedException {
        if (!pending.isEmpty()) {
            ship();
        }
    }

    /**
     * Serializes what has accumulated and sends it, waiting for credit first.
     */
    private void ship() throws InterruptedException {
        if (closed) {
            pending.clear();
            return;
        }

        // The wait that makes this flow control rather than hope. A receiver that has stopped
        // draining stops granting, and this sender stops here -- which, one hop at a time,
        // reaches all the way back to the source.
        if (!credit.tryAcquire(CREDIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            throw new IllegalStateException(
                    "no credit from subtask " + targetSubtask + " for "
                            + CREDIT_TIMEOUT_SECONDS + "s; the downstream task is stuck or gone");
        }

        try {
            byte[] payload = StreamElementSerializer.serialize(pending);
            outbound.onNext(DataBuffer.newBuilder()
                    .setSenderTaskId(senderTaskId)
                    .setTargetOperatorId(targetOperatorId)
                    .setTargetSubtask(targetSubtask)
                    .setPayload(ByteString.copyFrom(payload))
                    .setElementCount(pending.size())
                    .build());
            pending.clear();
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "could not serialize a batch for subtask " + targetSubtask, e);
        }
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            outbound.onCompleted();
        } catch (RuntimeException e) {
            log.debug("closing the stream to subtask {} failed", targetSubtask, e);
        }
    }
}
