package dev.dhruv.streaming.master;

import dev.dhruv.streaming.api.graph.JobGraph;
import dev.dhruv.streaming.master.graph.ExecutionGraph;
import dev.dhruv.streaming.metadata.EtcdMetadataStore;
import dev.dhruv.streaming.metadata.MetadataStore;
import dev.dhruv.streaming.metadata.RegisteredWorker;
import dev.dhruv.streaming.runtime.SerializationUtil;
import dev.dhruv.streaming.runtime.UserCodeClassLoader;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The master process: one gRPC server, a job, and a view of the cluster.
 *
 * <p>Deliberately a single point of failure, exactly as Flink's JobManager is without HA
 * configured. What makes that survivable rather than fatal is that the master keeps nothing
 * important only in memory: the job graph, the assignments and the job's state all live in etcd,
 * so a restarted master recovers rather than losing the job.
 *
 * <p>That is also what Demo 2 in Phase 7 shows: kill the master, watch the workers carry on
 * processing, restart it and watch it pick the job back up. Lost coordination is not lost work.
 */
public final class MasterBootstrap implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MasterBootstrap.class);

    private final int port;
    private final MetadataStore metadata;
    private final GrpcTaskDeployer deployer;
    private final JobMaster jobMaster;
    private final MasterService service;

    private Server server;
    private AutoCloseable workerWatch;

    /**
     * Creates a master.
     *
     * @param port          the control-plane port to serve on
     * @param etcdEndpoints where durable job state lives
     */
    public MasterBootstrap(int port, String etcdEndpoints) {
        this.port = port;
        this.metadata = new EtcdMetadataStore(etcdEndpoints);
        this.deployer = new GrpcTaskDeployer();
        this.jobMaster = new JobMaster(metadata, deployer);
        this.service = new MasterService(jobMaster, metadata);
    }

    /**
     * Starts the server and begins watching for workers.
     *
     * @throws IOException if the port cannot be bound
     */
    public void start() throws IOException {
        server = NettyServerBuilder.forPort(port)
                .addService(service)
                .maxInboundMessageSize(64 * 1024 * 1024)
                .build()
                .start();

        // Watch rather than poll. A worker appearing should be schedulable at once, and a worker
        // vanishing should reach the master without waiting out a polling interval.
        workerWatch = metadata.watchWorkers(workers ->
                log.info("cluster now has {} worker(s): {}",
                        workers.size(), workers.stream().map(RegisteredWorker::workerId).toList()));

        // Recover before accepting anything new, so that a restarted master knows what it was
        // already responsible for.
        jobMaster.recover().forEach((jobId, job) ->
                log.info("recovered job {} in state {} with {} assignment(s)",
                        jobId, job.state(), job.assignments().size()));

        log.info("master listening on port {}", port);
    }

    /**
     * Submits a job, waiting until the cluster is big enough to run it.
     *
     * @param graph        the validated logical graph
     * @param minimumWorkers how many workers to wait for
     * @param timeoutSeconds how long to wait before giving up
     * @return the compiled plan
     * @throws InterruptedException if interrupted while waiting
     */
    public ExecutionGraph submit(JobGraph graph, int minimumWorkers, long timeoutSeconds)
            throws InterruptedException {

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        List<RegisteredWorker> workers = metadata.listWorkers();
        while (workers.size() < minimumWorkers && System.nanoTime() < deadline) {
            log.info("waiting for workers: {} of {} registered", workers.size(), minimumWorkers);
            Thread.sleep(500);
            workers = metadata.listWorkers();
        }
        if (workers.size() < minimumWorkers) {
            throw new IllegalStateException("only " + workers.size() + " of " + minimumWorkers
                    + " workers registered within " + timeoutSeconds + "s");
        }

        return jobMaster.submit(graph, SerializationUtil.toBytes(graph));
    }

    /**
     * Returns the job master, for tests and for the status API in Phase 7.
     *
     * @return the job master
     */
    public JobMaster jobMaster() {
        return jobMaster;
    }

    /**
     * Blocks until the server stops.
     *
     * @throws InterruptedException if the waiting thread is interrupted
     */
    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    @Override
    public void close() {
        log.info("master shutting down");
        if (workerWatch != null) {
            try {
                workerWatch.close();
            } catch (Exception e) {
                log.debug("closing the worker watch failed", e);
            }
        }
        jobMaster.close();
        deployer.close();
        if (server != null) {
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
        metadata.close();
    }

    /**
     * Runs a master.
     *
     * @param args unused
     * @throws Exception if the master cannot start
     */
    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(env("MASTER_PORT", "9090"));
        String etcd = env("ETCD_ENDPOINTS", "http://localhost:2379");

        // The master deserializes the submitted JobGraph in order to compile it, and that graph
        // holds the user's own operators. So the master needs the job's classes exactly as every
        // worker does. Flink's JobManager has the same requirement, for the same reason.
        UserCodeClassLoader.configure(System.getenv("JOB_CLASSPATH"));

        MasterBootstrap master = new MasterBootstrap(port, etcd);
        Runtime.getRuntime().addShutdownHook(new Thread(master::close, "shutdown"));
        master.start();
        master.awaitTermination();
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
