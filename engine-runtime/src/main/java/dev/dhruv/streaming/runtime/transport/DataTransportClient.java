package dev.dhruv.streaming.runtime.transport;

import dev.dhruv.streaming.rpc.CreditGrant;
import dev.dhruv.streaming.rpc.DataBuffer;
import dev.dhruv.streaming.rpc.DataTransportServiceGrpc;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The sending end of the data plane: opens a record stream to a peer worker.
 *
 * <p>One gRPC connection per peer worker, shared by every channel to that peer, and one stream
 * within it per destination subtask. That is the arrangement credit exists to make safe --
 * sharing a connection is what lets one slow subtask stall its siblings unless something accounts
 * for each stream separately.
 */
public final class DataTransportClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DataTransportClient.class);

    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final int maxBufferElements;

    /**
     * Creates a client.
     *
     * @param maxBufferElements how many elements fill a buffer before it is shipped
     */
    public DataTransportClient(int maxBufferElements) {
        this.maxBufferElements = maxBufferElements;
    }

    /**
     * Opens a channel to one subtask on a peer worker.
     *
     * @param senderTaskId     this sender's wire identity
     * @param targetOperatorId which operator the destination runs
     * @param targetSubtask    which subtask of it
     * @param host             the peer's host
     * @param dataPort         the peer's data-plane port
     * @return the outgoing channel, which sends nothing until credit arrives
     */
    public ResultSubpartition openSubpartition(String senderTaskId,
                                               String targetOperatorId,
                                               int targetSubtask,
                                               String host,
                                               int dataPort) {

        ManagedChannel channel = channels.computeIfAbsent(host + ":" + dataPort, address -> {
            log.info("opening a data connection to {}", address);
            return NettyChannelBuilder.forAddress(host, dataPort)
                    .usePlaintext()
                    // Records are batched, and a batch of a few hundred user objects under Java
                    // serialization is comfortably larger than gRPC's 4MB default.
                    .maxInboundMessageSize(64 * 1024 * 1024)
                    .build();
        });

        // The subpartition needs the request stream, and the credit handler needs the
        // subpartition to grant credit to. One of the two has to exist first, so the handler
        // holds a reference filled in a few lines later -- safely, because no credit can arrive
        // until the stream has actually been opened.
        AtomicReference<RemoteSubpartition> subpartitionRef = new AtomicReference<>();

        StreamObserver<DataBuffer> outbound =
                openDetached(channel, targetOperatorId, targetSubtask, subpartitionRef);

        RemoteSubpartition subpartition = Subpartitions.remote(
                senderTaskId, targetOperatorId, targetSubtask, outbound, maxBufferElements);
        subpartitionRef.set(subpartition);
        return subpartition;
    }

    /**
     * Opens the stream detached from whatever gRPC {@link Context} is current.
     *
     * <h2>Why this is not incidental</h2>
     *
     * <p>These streams are opened while the worker is handling the master's {@code DeployTask}
     * call. A gRPC client call started inside a server handler inherits that handler's Context by
     * default -- and a server Context is cancelled the instant the handler returns.
     *
     * <p>So the record stream, which has to live for the whole job, is destroyed within
     * milliseconds of the deployment that created it. The symptom is worth recognising because
     * it explains nothing on its own: every process healthy, the job reported as RUNNING, no
     * errors beyond a single {@code CANCELLED: io.grpc.Context was cancelled without error}, and
     * not one record arriving anywhere.
     *
     * <p>{@link Context#ROOT} has no deadline and no cancellation, which is exactly what a
     * connection that must outlive the call that created it needs.
     */
    private StreamObserver<DataBuffer> openDetached(
            ManagedChannel channel,
            String targetOperatorId,
            int targetSubtask,
            AtomicReference<RemoteSubpartition> subpartitionRef) {

        Context previous = Context.ROOT.attach();
        try {
            return DataTransportServiceGrpc.newStub(channel)
                    .exchangeRecords(new StreamObserver<>() {

                        @Override
                        public void onNext(CreditGrant grant) {
                            RemoteSubpartition subpartition = subpartitionRef.get();
                            if (subpartition != null) {
                                subpartition.grantCredit(grant.getBuffers());
                            }
                        }

                        @Override
                        public void onError(Throwable error) {
                            // The peer is gone, or the destination task failed. The sending task
                            // notices when it next waits for credit that never comes, and fails
                            // with a message naming the subtask rather than a stack trace from
                            // inside gRPC.
                            log.warn("record stream to {}#{} failed: {}",
                                    targetOperatorId, targetSubtask, error.getMessage());
                        }

                        @Override
                        public void onCompleted() {
                            log.debug("record stream to {}#{} closed",
                                    targetOperatorId, targetSubtask);
                        }
                    });
        } finally {
            Context.ROOT.detach(previous);
        }
    }

    @Override
    public void close() {
        channels.values().forEach(channel -> {
            channel.shutdown();
            try {
                channel.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                channel.shutdownNow();
            }
        });
        channels.clear();
    }
}
