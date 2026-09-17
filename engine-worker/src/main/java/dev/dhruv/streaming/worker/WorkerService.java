package dev.dhruv.streaming.worker;

import dev.dhruv.streaming.rpc.CheckpointId;
import dev.dhruv.streaming.rpc.CheckpointTrigger;
import dev.dhruv.streaming.rpc.DeployAck;
import dev.dhruv.streaming.rpc.Empty;
import dev.dhruv.streaming.rpc.RestoreRequest;
import dev.dhruv.streaming.rpc.TaskDeployment;
import dev.dhruv.streaming.rpc.TaskId;
import dev.dhruv.streaming.rpc.WorkerServiceGrpc;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * What the master can ask this worker to do.
 *
 * <p>Thin on purpose: every method turns a protobuf message into a call on
 * {@link TaskManager} and returns. The work happens on task threads, not on gRPC handler
 * threads, so a deployment that takes a moment to start does not hold up the master.
 */
final class WorkerService extends WorkerServiceGrpc.WorkerServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(WorkerService.class);

    private final TaskManager taskManager;

    WorkerService(TaskManager taskManager) {
        this.taskManager = taskManager;
    }

    @Override
    public void deployTask(TaskDeployment deployment, StreamObserver<DeployAck> response) {
        try {
            taskManager.deploy(deployment);
            response.onNext(DeployAck.newBuilder().setAccepted(true).build());
        } catch (Exception e) {
            log.error("failed to deploy a task", e);
            response.onNext(DeployAck.newBuilder()
                    .setAccepted(false)
                    .setReason(e.getClass().getSimpleName() + ": " + e.getMessage())
                    .build());
        }
        response.onCompleted();
    }

    @Override
    public void cancelTask(TaskId taskId, StreamObserver<Empty> response) {
        taskManager.cancel(taskId.getOperatorId(), taskId.getSubtaskIndex());
        response.onNext(Empty.getDefaultInstance());
        response.onCompleted();
    }

    @Override
    public void triggerCheckpoint(CheckpointTrigger trigger, StreamObserver<Empty> response) {
        // Phase 4. The coordinator will call this on source tasks only; the source injects a
        // barrier into its output and the barrier does the rest of the work by travelling.
        log.warn("checkpoint {} requested, but checkpointing arrives in Phase 4",
                trigger.getCheckpointId());
        response.onNext(Empty.getDefaultInstance());
        response.onCompleted();
    }

    @Override
    public void notifyCheckpointComplete(CheckpointId id, StreamObserver<Empty> response) {
        // Phase 4 delivers this to sinks, where in Phase 6 it becomes the commit half of
        // two-phase commit against Iceberg.
        log.warn("checkpoint {} completion notified, but checkpointing arrives in Phase 4",
                id.getCheckpointId());
        response.onNext(Empty.getDefaultInstance());
        response.onCompleted();
    }

    @Override
    public void restoreTask(RestoreRequest request, StreamObserver<Empty> response) {
        // Phase 4. A task will be redeployed with a state handle and restore from it before
        // processing anything.
        log.warn("restore requested for {}, but recovery arrives in Phase 4",
                request.getTaskId().getOperatorId());
        response.onNext(Empty.getDefaultInstance());
        response.onCompleted();
    }
}
