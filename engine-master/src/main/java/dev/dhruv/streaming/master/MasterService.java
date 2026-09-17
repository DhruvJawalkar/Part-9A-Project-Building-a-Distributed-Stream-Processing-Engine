package dev.dhruv.streaming.master;

import dev.dhruv.streaming.api.graph.JobGraph;
import dev.dhruv.streaming.master.graph.ExecutionGraph;
import dev.dhruv.streaming.metadata.MetadataStore;
import dev.dhruv.streaming.metadata.RegisteredWorker;
import dev.dhruv.streaming.runtime.SerializationUtil;
import dev.dhruv.streaming.rpc.CheckpointAck;
import dev.dhruv.streaming.rpc.Empty;
import dev.dhruv.streaming.rpc.MasterCommand;
import dev.dhruv.streaming.rpc.MasterServiceGrpc;
import dev.dhruv.streaming.rpc.JobSubmission;
import dev.dhruv.streaming.rpc.RegisterAck;
import dev.dhruv.streaming.rpc.SubmitAck;
import dev.dhruv.streaming.rpc.TaskFailure;
import dev.dhruv.streaming.rpc.WorkerBeat;
import dev.dhruv.streaming.rpc.WorkerInfo;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * What workers can say to the master.
 *
 * <p>Three of the four methods do almost nothing, which is the intended shape. The master's
 * judgement lives in {@link JobMaster} and {@link TaskTracker}; this class is the place where
 * protobuf stops and those begin.
 */
final class MasterService extends MasterServiceGrpc.MasterServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(MasterService.class);

    private final JobMaster jobMaster;
    private final MetadataStore metadata;

    /** Open heartbeat streams, so the master can send a command back down one. */
    private final Map<String, StreamObserver<MasterCommand>> commandStreams =
            new ConcurrentHashMap<>();

    MasterService(JobMaster jobMaster, MetadataStore metadata) {
        this.jobMaster = jobMaster;
        this.metadata = metadata;
    }

    @Override
    public void submitJob(JobSubmission submission, StreamObserver<SubmitAck> response) {
        try {
            List<RegisteredWorker> workers = awaitWorkers(
                    submission.getMinimumWorkers(), submission.getWaitTimeoutMillis());

            byte[] serialized = submission.getSerializedGraph().toByteArray();
            JobGraph graph = SerializationUtil.fromBytes(serialized);
            ExecutionGraph plan = jobMaster.submit(graph, serialized);

            SubmitAck.Builder ack = SubmitAck.newBuilder()
                    .setAccepted(true)
                    .setJobId(graph.jobId())
                    .setTaskCount(plan.taskCount())
                    .addAllWorkerIds(plan.workerIds());
            plan.assignments().entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> ack.addAssignments(
                            entry.getKey() + " -> " + entry.getValue().workerId()));

            response.onNext(ack.build());
        } catch (Exception e) {
            log.error("job submission failed", e);
            response.onNext(SubmitAck.newBuilder()
                    .setAccepted(false)
                    .setReason(e.getClass().getSimpleName() + ": " + e.getMessage())
                    .build());
        }
        response.onCompleted();
    }

    private List<RegisteredWorker> awaitWorkers(int minimum, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        List<RegisteredWorker> workers = metadata.listWorkers();
        while (workers.size() < minimum && System.nanoTime() < deadline) {
            log.info("waiting for workers: {} of {} registered", workers.size(), minimum);
            Thread.sleep(500);
            workers = metadata.listWorkers();
        }
        if (workers.size() < minimum) {
            throw new IllegalStateException("only " + workers.size() + " of " + minimum
                    + " workers registered within " + timeoutMillis + "ms");
        }
        return workers;
    }

    @Override
    public void registerWorker(WorkerInfo info, StreamObserver<RegisterAck> response) {
        log.info("worker {} registered from {}:{} (data {}), {} slots",
                info.getWorkerId(), info.getHost(), info.getRpcPort(),
                info.getDataPort(), info.getSlots());

        jobMaster.taskTracker().workerRegistered(info.getWorkerId());

        response.onNext(RegisterAck.newBuilder()
                .setAccepted(true)
                .setHeartbeatIntervalMillis(TaskTracker.HEARTBEAT_INTERVAL.toMillis())
                .build());
        response.onCompleted();
    }

    @Override
    public StreamObserver<WorkerBeat> heartbeat(StreamObserver<MasterCommand> commands) {
        return new StreamObserver<>() {

            private String workerId;

            @Override
            public void onNext(WorkerBeat beat) {
                workerId = beat.getWorkerId();
                commandStreams.putIfAbsent(workerId, commands);
                jobMaster.taskTracker().heartbeatReceived(workerId);
            }

            @Override
            public void onError(Throwable error) {
                // The stream broke. Deliberately not treated as death on its own: the tracker
                // decides that, from silence, on one rule. A worker whose connection blipped and
                // reconnected within the window is alive, and a worker that stopped beating is
                // dead whether or not its stream reported an error.
                log.debug("heartbeat stream from {} failed: {}", workerId, error.getMessage());
                commandStreams.remove(workerId);
            }

            @Override
            public void onCompleted() {
                // A clean goodbye: the worker is shutting down on purpose.
                log.info("worker {} closed its heartbeat stream", workerId);
                commandStreams.remove(workerId);
                if (workerId != null) {
                    jobMaster.taskTracker().workerDeregistered(workerId);
                }
                commands.onCompleted();
            }
        };
    }

    @Override
    public void acknowledgeCheckpoint(CheckpointAck ack, StreamObserver<Empty> response) {
        // Phase 4. The coordinator collects these and completes a checkpoint once every task
        // has acknowledged.
        log.debug("checkpoint ack from {} for checkpoint {}",
                ack.getWorkerId(), ack.getCheckpointId());
        response.onNext(Empty.getDefaultInstance());
        response.onCompleted();
    }

    @Override
    public void reportTaskFailure(TaskFailure failure, StreamObserver<Empty> response) {
        String taskId = failure.getTaskId().getOperatorId()
                + ":" + failure.getTaskId().getSubtaskIndex();
        log.error("worker {} reported that task {} failed: {}",
                failure.getWorkerId(), taskId, failure.getMessage());

        jobMaster.onTaskFailure(failure.getTaskId().getJobId(), taskId, failure.getMessage());

        response.onNext(Empty.getDefaultInstance());
        response.onCompleted();
    }

    /**
     * Sends a command to a worker, down the heartbeat stream it already has open.
     *
     * @param workerId the worker
     * @param command  what to tell it
     */
    void send(String workerId, MasterCommand command) {
        StreamObserver<MasterCommand> stream = commandStreams.get(workerId);
        if (stream == null) {
            log.debug("no open stream to {}; dropping a command", workerId);
            return;
        }
        try {
            stream.onNext(command);
        } catch (RuntimeException e) {
            log.warn("could not send a command to {}", workerId, e);
            commandStreams.remove(workerId);
        }
    }
}
