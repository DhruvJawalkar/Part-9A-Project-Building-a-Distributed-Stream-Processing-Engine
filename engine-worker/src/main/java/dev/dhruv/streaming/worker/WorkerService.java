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
        try {
            // The coordinator invokes this only on workers with source tasks. SourceTask writes
            // its next input position, forwards the marker in band, then sends its ack.
            taskManager.triggerCheckpoint(trigger);
            response.onNext(Empty.getDefaultInstance());
            response.onCompleted();
        } catch (Exception e) {
            log.error("could not trigger checkpoint {}", trigger.getCheckpointId(), e);
            response.onError(e);
        }
    }

    @Override
    public void notifyCheckpointComplete(CheckpointId id, StreamObserver<Empty> response) {
        try {
            // Do not acknowledge the gRPC call until the sink callback has completed. Otherwise
            // a failed external commit would look successful to the master.
            taskManager.notifyCheckpointComplete(id.getJobId(), id.getCheckpointId());
            response.onNext(Empty.getDefaultInstance());
            response.onCompleted();
        } catch (Exception failure) {
            log.error("could not complete checkpoint {}", id.getCheckpointId(), failure);
            response.onError(failure);
        }
    }

    @Override
    public void abortCheckpoint(CheckpointId id, StreamObserver<Empty> response) {
        taskManager.abortCheckpoint(id.getJobId(), id.getCheckpointId());
        response.onNext(Empty.getDefaultInstance());
        response.onCompleted();
    }

    @Override
    public void restoreTask(RestoreRequest request, StreamObserver<Empty> response) {
        // A state handle is applied from TaskDeployment before its thread starts. Restoring an
        // already-running task would splice a checkpoint into a live input stream, so recovery
        // redeploys the whole job instead of using this legacy RPC as an in-place mutation.
        response.onError(new IllegalStateException("restore is applied on redeployment via "
                + "TaskDeployment.state_handle_uri, not to a running task"));
    }
}
