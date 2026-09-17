package dev.dhruv.streaming.runtime.transport;

import dev.dhruv.streaming.api.StreamElement;
import dev.dhruv.streaming.rpc.CreditGrant;
import dev.dhruv.streaming.rpc.DataBuffer;
import dev.dhruv.streaming.rpc.DataTransportServiceGrpc;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * The receiving end of the data plane: buffers arrive here and become elements in input gates.
 *
 * <h2>How credit is actually granted</h2>
 *
 * <p>The accounting is deliberately as simple as it can be while still being real. A sender
 * begins with {@link #INITIAL_CREDIT} buffers of permission. Each buffer that arrives is
 * unpacked and its elements pushed into the destination gate; once they are all in, one credit
 * is granted back.
 *
 * <p>The subtlety is in the word "once". Pushing into a gate <em>blocks</em> when that channel's
 * queue is full, so a receiver that has stopped consuming does not return credit, and its sender
 * stops as soon as its initial allowance is spent. Nothing needs to measure queue depth or
 * predict demand: the backpressure falls out of the same bounded queue that provides it inside a
 * single process, and credit simply carries the effect across the network.
 *
 * <p>That blocking is also why this server is given a thread pool that can grow. A handler
 * thread parked on a full gate is the mechanism working, not a stall to be avoided -- but it
 * does mean a fixed pool could be exhausted by slow receivers and deadlock the ones that would
 * have drained them.
 */
public final class DataTransportService extends DataTransportServiceGrpc.DataTransportServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(DataTransportService.class);

    /**
     * How many buffers a sender may have in flight before hearing back.
     *
     * <p>Small on purpose. Enough that a sender is never idle waiting for a round trip, few
     * enough that a stalled receiver's backlog is bounded by a handful of buffers rather than by
     * however much the network was willing to accept.
     */
    public static final int INITIAL_CREDIT = 4;

    private final TaskInputRegistry registry;

    /**
     * Creates the service.
     *
     * @param registry where this worker's running tasks and their gates are looked up
     */
    public DataTransportService(TaskInputRegistry registry) {
        this.registry = registry;
    }

    @Override
    public StreamObserver<DataBuffer> exchangeRecords(StreamObserver<CreditGrant> credits) {
        // The opening grant, sent the moment the stream exists and before any data has arrived.
        //
        // This ordering is the whole protocol, and getting it backwards deadlocks the job in a
        // way that looks like nothing happening at all: a sender starts at zero credit and waits
        // for permission, so a receiver that only granted credit in response to a buffer would
        // be waiting for a buffer that the sender is not allowed to send. Both sides idle, no
        // error anywhere, and every process healthy.
        credits.onNext(CreditGrant.newBuilder().setBuffers(INITIAL_CREDIT).build());

        return new StreamObserver<>() {

            @Override
            public void onNext(DataBuffer buffer) {
                try {
                    deliver(buffer);
                    grant(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    credits.onError(e);
                } catch (Exception e) {
                    log.error("failed to deliver a buffer from {} to {}#{}",
                            buffer.getSenderTaskId(), buffer.getTargetOperatorId(),
                            buffer.getTargetSubtask(), e);
                    credits.onError(e);
                }
            }

            @Override
            public void onError(Throwable error) {
                // The sender's task ended or its worker died. Either way there is nothing to
                // clean up here: the gate belongs to the receiving task, which is unaffected.
                log.debug("a record stream failed", error);
            }

            @Override
            public void onCompleted() {
                credits.onCompleted();
            }

            private void grant(int buffers) {
                credits.onNext(CreditGrant.newBuilder().setBuffers(buffers).build());
            }
        };
    }

    private void deliver(DataBuffer buffer) throws Exception {
        var destination = registry.resolve(
                buffer.getTargetOperatorId(), buffer.getTargetSubtask(), buffer.getSenderTaskId());

        if (destination.isEmpty()) {
            // Legitimate rather than exceptional: a buffer can arrive moments after its
            // destination task was cancelled. Dropping it is correct -- the job is being torn
            // down, and in Phase 4 it will be restarted from a checkpoint that predates this
            // record anyway.
            log.debug("dropping a buffer for {}#{}: no such task running here",
                    buffer.getTargetOperatorId(), buffer.getTargetSubtask());
            return;
        }

        List<StreamElement> elements =
                StreamElementSerializer.deserialize(buffer.getPayload().toByteArray());

        InputGate gate = destination.get().gate();
        int channel = destination.get().channelIndex();
        for (StreamElement element : elements) {
            // Blocks when the channel is full. That is the whole of backpressure: this handler
            // waits, so no credit is returned, so the sender waits too.
            gate.enqueue(channel, element);
        }
    }
}
