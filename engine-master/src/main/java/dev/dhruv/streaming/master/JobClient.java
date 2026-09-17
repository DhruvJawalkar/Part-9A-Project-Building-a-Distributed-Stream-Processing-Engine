package dev.dhruv.streaming.master;

import com.google.protobuf.ByteString;
import dev.dhruv.streaming.api.graph.JobGraph;
import dev.dhruv.streaming.rpc.JobSubmission;
import dev.dhruv.streaming.rpc.MasterServiceGrpc;
import dev.dhruv.streaming.rpc.SubmitAck;
import dev.dhruv.streaming.runtime.SerializationUtil;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Submits a job to a running cluster.
 *
 * <p>The client's whole job is to serialize the graph and hand it over. It does not compile, it
 * does not schedule, and it does not talk to any worker -- which is the point.
 *
 * <h2>Why the client does not simply do it itself</h2>
 *
 * <p>An earlier version of this class compiled the graph and deployed the tasks directly, which
 * worked and was fewer moving parts. It was also wrong in a way that only showed up when a worker
 * died: the master had never been told the job existed, so when its failure detector fired there
 * was nothing to fail. The job carried on half-running with nobody supervising it.
 *
 * <p>That is the argument for a control plane in one sentence. Whoever is responsible for
 * reacting to failure has to be the one that knows what is running.
 */
public final class JobClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(JobClient.class);

    private final ManagedChannel channel;
    private final MasterServiceGrpc.MasterServiceBlockingStub master;

    /**
     * Connects to a master.
     *
     * @param masterHost the master's host
     * @param masterPort the master's control-plane port
     */
    public JobClient(String masterHost, int masterPort) {
        this.channel = NettyChannelBuilder.forAddress(masterHost, masterPort)
                .usePlaintext()
                // A submission carries the serialized job graph, including the user's operators.
                .maxInboundMessageSize(64 * 1024 * 1024)
                .build();
        this.master = MasterServiceGrpc.newBlockingStub(channel);
    }

    /**
     * Submits a job and waits for the master to schedule it.
     *
     * @param graph          the validated logical graph
     * @param minimumWorkers how many workers the master should wait for before scheduling
     * @param timeoutSeconds how long it should wait for them
     * @return what the master decided
     * @throws IllegalStateException if the master refused the job
     */
    public SubmitAck submit(JobGraph graph, int minimumWorkers, long timeoutSeconds) {
        log.info("submitting '{}' to the master", graph.name());

        SubmitAck ack = master
                .withDeadlineAfter(timeoutSeconds + 60, TimeUnit.SECONDS)
                .submitJob(JobSubmission.newBuilder()
                        .setSerializedGraph(
                                ByteString.copyFrom(SerializationUtil.toBytes(graph)))
                        .setMinimumWorkers(minimumWorkers)
                        .setWaitTimeoutMillis(TimeUnit.SECONDS.toMillis(timeoutSeconds))
                        .build());

        if (!ack.getAccepted()) {
            throw new IllegalStateException("the master refused the job: " + ack.getReason());
        }
        return ack;
    }

    @Override
    public void close() {
        channel.shutdown();
        try {
            if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            channel.shutdownNow();
        }
    }
}
