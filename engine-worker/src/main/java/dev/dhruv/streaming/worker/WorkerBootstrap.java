package dev.dhruv.streaming.worker;

import dev.dhruv.streaming.metadata.RegisteredWorker;
import dev.dhruv.streaming.metadata.EtcdMetadataStore;
import dev.dhruv.streaming.metadata.MetadataStore;
import dev.dhruv.streaming.rpc.MasterServiceGrpc;
import dev.dhruv.streaming.rpc.TaskFailure;
import dev.dhruv.streaming.rpc.TaskId;
import dev.dhruv.streaming.rpc.WorkerInfo;
import dev.dhruv.streaming.runtime.transport.DataTransportClient;
import dev.dhruv.streaming.runtime.transport.DataTransportService;
import dev.dhruv.streaming.runtime.transport.Subpartitions;
import dev.dhruv.streaming.runtime.UserCodeClassLoader;
import dev.dhruv.streaming.runtime.transport.TaskInputRegistry;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A worker process: two gRPC servers, a registration, and a heartbeat.
 *
 * <p>Start three of these and a cluster exists. Nothing coordinates them with each other; each
 * one registers in etcd under a lease and tells the master it is alive, and the master's view of
 * the cluster is whatever is currently in that prefix.
 *
 * <h2>Two servers, two ports</h2>
 *
 * <p>The control plane and the data plane listen separately, and the separation is the point.
 * Records saturate the data port under load. If heartbeats shared it, a worker doing a great deal
 * of useful work would look exactly like a worker that had died, and the master would start
 * failing healthy jobs at precisely the moment the cluster was busiest.
 */
public final class WorkerBootstrap implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(WorkerBootstrap.class);

    /** How long a worker's etcd registration survives without renewal. */
    private static final long LEASE_TTL_SECONDS = 10;

    private final String workerId;
    private final String host;
    private final int rpcPort;
    private final int dataPort;
    private final int slots;

    private final MetadataStore metadata;
    private final TaskInputRegistry inputRegistry = new TaskInputRegistry();
    private final DataTransportClient transportClient;
    private final TaskManager taskManager;

    private Server controlServer;
    private Server dataServer;
    private MetadataStore.WorkerRegistration registration;
    private HeartbeatClient heartbeat;
    private MasterServiceGrpc.MasterServiceBlockingStub masterBlocking;

    /**
     * Creates a worker.
     *
     * @param workerId      unique id, usually from the WORKER_ID environment variable
     * @param host          the address peers and the master should use to reach this worker
     * @param rpcPort       port for WorkerService
     * @param dataPort      port for DataTransportService
     * @param slots         how many vertical pipeline slices this worker will accept
     * @param etcdEndpoints where to register
     */
    public WorkerBootstrap(String workerId, String host, int rpcPort, int dataPort, int slots,
                           String etcdEndpoints) {
        this.workerId = workerId;
        this.host = host;
        this.rpcPort = rpcPort;
        this.dataPort = dataPort;
        this.slots = slots;
        this.metadata = new EtcdMetadataStore(etcdEndpoints);
        this.transportClient = new DataTransportClient(Subpartitions.DEFAULT_MAX_BUFFER_ELEMENTS);
        this.taskManager = new TaskManager(workerId, host, dataPort, inputRegistry, transportClient);
    }

    /**
     * Starts both servers, registers, and begins beating.
     *
     * @param masterHost the master's host
     * @param masterPort the master's control-plane port
     * @throws IOException if either port cannot be bound
     */
    public void start(String masterHost, int masterPort) throws IOException {
        controlServer = NettyServerBuilder.forPort(rpcPort)
                .addService(new WorkerService(taskManager))
                .build()
                .start();

        dataServer = NettyServerBuilder.forPort(dataPort)
                .addService(new DataTransportService(inputRegistry))
                .maxInboundMessageSize(64 * 1024 * 1024)
                // A cached pool rather than a fixed one, because a handler thread parks while
                // pushing into a full input gate. That parking is backpressure working, but a
                // fixed pool could be exhausted by slow receivers and deadlock the very tasks
                // that would have drained them.
                .executor(Executors.newCachedThreadPool())
                .build()
                .start();

        log.info("worker {} listening: control {}:{}, data {}:{}",
                workerId, host, rpcPort, host, dataPort);

        var masterChannel = NettyChannelBuilder.forAddress(masterHost, masterPort)
                .usePlaintext()
                .build();
        var masterAsync = MasterServiceGrpc.newStub(masterChannel);
        this.masterBlocking = MasterServiceGrpc.newBlockingStub(masterChannel);

        masterBlocking.registerWorker(WorkerInfo.newBuilder()
                .setWorkerId(workerId)
                .setHost(host)
                .setRpcPort(rpcPort)
                .setDataPort(dataPort)
                .setSlots(slots)
                .build());

        // etcd after the master, so that a worker only appears schedulable once it is actually
        // able to accept a deployment.
        registration = metadata.registerWorker(
                new RegisteredWorker(workerId, host, rpcPort, dataPort, slots), LEASE_TTL_SECONDS);

        taskManager.onTaskFailure(this::reportTaskFailure);

        heartbeat = new HeartbeatClient(workerId, "", masterAsync, taskManager);
        heartbeat.start();

        log.info("worker {} registered with the master at {}:{} and with etcd",
                workerId, masterHost, masterPort);
    }

    private void reportTaskFailure(String taskKey, Throwable failure) {
        // Operator ids are user supplied and may legitimately contain '#'. The separator is
        // therefore the final one, immediately before the numeric subtask index.
        int separator = taskKey.lastIndexOf('#');
        if (separator < 1 || separator == taskKey.length() - 1) {
            log.error("could not parse task identity '{}' while reporting failure", taskKey);
            return;
        }
        String operatorId = taskKey.substring(0, separator);
        int subtaskIndex;
        try {
            subtaskIndex = Integer.parseInt(taskKey.substring(separator + 1));
        } catch (NumberFormatException e) {
            log.error("could not parse task identity '{}' while reporting failure", taskKey, e);
            return;
        }
        StringWriter stack = new StringWriter();
        failure.printStackTrace(new PrintWriter(stack));

        try {
            masterBlocking.reportTaskFailure(TaskFailure.newBuilder()
                    .setWorkerId(workerId)
                    .setTaskId(TaskId.newBuilder()
                            .setOperatorId(operatorId)
                            .setSubtaskIndex(subtaskIndex))
                    .setMessage(failure.getClass().getSimpleName() + ": " + failure.getMessage())
                    .setStackTrace(stack.toString())
                    .build());
        } catch (Exception e) {
            log.error("could not report the failure of {} to the master", taskKey, e);
        }
    }

    /**
     * Blocks until the servers stop.
     *
     * @throws InterruptedException if the waiting thread is interrupted
     */
    public void awaitTermination() throws InterruptedException {
        controlServer.awaitTermination();
    }

    @Override
    public void close() {
        log.info("worker {} shutting down", workerId);
        if (heartbeat != null) {
            heartbeat.close();
        }
        // Deregistering before stopping the servers, so the master learns this was deliberate
        // rather than watching the lease expire and calling it a crash.
        if (registration != null) {
            registration.close();
        }
        taskManager.close();
        transportClient.close();
        shutdown(dataServer);
        shutdown(controlServer);
        metadata.close();
    }

    private static void shutdown(Server server) {
        if (server == null) {
            return;
        }
        server.shutdown();
        try {
            if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
                server.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            server.shutdownNow();
        }
    }

    /**
     * Runs a worker.
     *
     * <p>Configured entirely from the environment, so that {@code docker compose} can start three
     * of these with nothing but different variables.
     *
     * @param args unused
     * @throws Exception if the worker cannot start
     */
    public static void main(String[] args) throws Exception {
        String workerId = env("WORKER_ID", "worker-1");
        String host = env("WORKER_HOST", "localhost");
        int rpcPort = Integer.parseInt(env("WORKER_RPC_PORT", "9091"));
        int dataPort = Integer.parseInt(env("WORKER_DATA_PORT", "9191"));
        int slots = Integer.parseInt(env("WORKER_SLOTS", "8"));
        String etcd = env("ETCD_ENDPOINTS", "http://localhost:2379");
        String masterHost = env("MASTER_HOST", "localhost");
        int masterPort = Integer.parseInt(env("MASTER_PORT", "9090"));

        // Before anything else: the worker must be able to load the job's classes, which it was
        // not compiled against. See UserCodeClassLoader for why this is not optional.
        UserCodeClassLoader.configure(System.getenv("JOB_CLASSPATH"));

        WorkerBootstrap worker =
                new WorkerBootstrap(workerId, host, rpcPort, dataPort, slots, etcd);
        Runtime.getRuntime().addShutdownHook(new Thread(worker::close, "shutdown"));
        worker.start(masterHost, masterPort);
        worker.awaitTermination();
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
