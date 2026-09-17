package dev.dhruv.streaming.runtime.transport;

import dev.dhruv.streaming.rpc.DataBuffer;
import io.grpc.stub.StreamObserver;

/**
 * Creates outgoing channels.
 *
 * <p>A factory rather than public constructors, so that {@link LocalSubpartition} and
 * {@link RemoteSubpartition} stay package-private. Choosing between them is the deployment's
 * decision, made once when a task is set up; nothing afterwards should be able to hold one and
 * ask which kind it is.
 */
public final class Subpartitions {

    /**
     * How many elements fill a buffer before it is shipped without waiting for the timeout.
     *
     * <p>The other half of the batching policy, alongside {@code bufferTimeoutMs}. Whichever
     * limit is reached first wins: a busy channel ships on size and never waits, a quiet one
     * ships on time and never stalls.
     */
    public static final int DEFAULT_MAX_BUFFER_ELEMENTS = 256;

    private Subpartitions() {
    }

    /**
     * Creates a channel to a task on this same worker.
     *
     * @param channelIndex which of the destination's input channels the sender occupies
     * @param destination  the destination's input gate
     * @return the channel
     */
    public static ResultSubpartition local(int channelIndex, InputGate destination) {
        return new LocalSubpartition(channelIndex, destination);
    }

    /**
     * Creates a channel to a task on another worker.
     *
     * @param senderTaskId      identifies the sender in log lines and on the wire
     * @param targetOperatorId  which operator the destination runs
     * @param targetSubtask     which downstream subtask this channel feeds
     * @param outbound          the gRPC request stream to send buffers on
     * @param maxBufferElements how many elements fill a buffer
     * @return the channel, which will not send anything until the receiver grants credit
     */
    public static RemoteSubpartition remote(String senderTaskId,
                                            String targetOperatorId,
                                            int targetSubtask,
                                            StreamObserver<DataBuffer> outbound,
                                            int maxBufferElements) {
        return new RemoteSubpartition(
                senderTaskId, targetOperatorId, targetSubtask, outbound, maxBufferElements);
    }
}
