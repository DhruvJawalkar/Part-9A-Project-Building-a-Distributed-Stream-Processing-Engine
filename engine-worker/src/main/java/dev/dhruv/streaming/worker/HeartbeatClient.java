package dev.dhruv.streaming.worker;

import dev.dhruv.streaming.rpc.MasterCommand;
import dev.dhruv.streaming.rpc.MasterServiceGrpc;
import dev.dhruv.streaming.rpc.TaskId;
import dev.dhruv.streaming.rpc.TaskState;
import dev.dhruv.streaming.rpc.TaskStatus;
import dev.dhruv.streaming.rpc.SourcePartitionLag;
import dev.dhruv.streaming.rpc.WorkerBeat;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tells the master this worker is alive, once a second, and listens for what it says back.
 *
 * <h2>Why the stream carries traffic in both directions</h2>
 *
 * <p>Beats go up and commands come down the same long-lived stream. The master could instead
 * open its own connection to a worker when it had something to say, and the reason not to is
 * worth stating: it would mean the master needs to be able to reach every worker, on an address
 * the worker advertised, at any moment. Workers behind NAT, in containers with ephemeral
 * addresses, or on a network that only routes one way all break that assumption.
 *
 * <p>A worker-initiated stream inverts it. The worker only has to be able to reach the master,
 * which it must be able to do anyway in order to register, and the master gets a reply channel
 * for free.
 *
 * <h2>What a beat carries</h2>
 *
 * <p>Liveness is the point, but a beat that travels every second is also the cheapest possible
 * carrier for per-task metrics, so it takes them along: records in and out, and from Phase 4 the
 * checkpoint cost figures. That is what makes the status API in Phase 7 able to answer questions
 * about a running job without asking anybody anything.
 */
final class HeartbeatClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatClient.class);

    static final Duration INTERVAL = Duration.ofSeconds(1);

    private final String workerId;
    private final MasterServiceGrpc.MasterServiceStub master;
    private final TaskManager taskManager;
    private final ScheduledExecutorService scheduler;

    private volatile StreamObserver<WorkerBeat> beats;
    private final AtomicBoolean reconnectScheduled = new AtomicBoolean();
    private volatile boolean closed;

    HeartbeatClient(String workerId,
                    MasterServiceGrpc.MasterServiceStub master,
                    TaskManager taskManager) {
        this.workerId = workerId;
        this.master = master;
        this.taskManager = taskManager;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "heartbeat");
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * Opens the stream and starts beating.
     */
    void start() {
        openStream();
        scheduler.scheduleAtFixedRate(this::beat, 0, INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
        log.info("heartbeating to the master every {}ms", INTERVAL.toMillis());
    }

    private void openStream() {
        if (closed) {
            return;
        }
        beats = master.heartbeat(new StreamObserver<>() {
            @Override
            public void onNext(MasterCommand command) {
                handle(command);
            }

            @Override
            public void onError(Throwable error) {
                // The master has gone. The worker deliberately keeps running: its tasks are
                // processing records and stopping them would throw away work that a restarted
                // master could still make use of. Losing coordination is not losing work --
                // which is exactly what Demo 2 in Phase 7 sets out to show.
                log.warn("lost the heartbeat stream to the master: {}", error.getMessage());
                beats = null;
                scheduleReconnect();
            }

            @Override
            public void onCompleted() {
                log.info("the master closed the heartbeat stream; reconnecting");
                beats = null;
                scheduleReconnect();
            }
        });
        reconnectScheduled.set(false);
    }

    private void scheduleReconnect() {
        if (!closed && reconnectScheduled.compareAndSet(false, true)) {
            scheduler.schedule(this::openStream, INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private void beat() {
        try {
            StreamObserver<WorkerBeat> stream = beats;
            if (stream == null) {
                scheduleReconnect();
                return;
            }
            WorkerBeat.Builder builder = WorkerBeat.newBuilder().setWorkerId(workerId);

            long totalRecords = 0;
            for (RunningTask task : taskManager.runningTasks().values()) {
                totalRecords += task.recordsIn();
                TaskStatus.Builder status = TaskStatus.newBuilder()
                        .setTaskId(TaskId.newBuilder()
                                .setJobId(task.jobId())
                                .setOperatorId(task.operatorId())
                                .setSubtaskIndex(task.subtaskIndex()))
                        .setState(task.isFinished()
                                ? TaskState.TASK_FINISHED : TaskState.TASK_RUNNING)
                        .setRecordsIn(task.recordsIn())
                        .setRecordsOut(task.recordsOut())
                        .setLastCheckpointId(task.lastCheckpointId())
                        .setLastCheckpointDurationMillis(task.lastCheckpointDurationMillis())
                        .setLastCheckpointStateBytes(task.lastCheckpointStateBytes())
                        .setLastAlignmentMillis(task.lastAlignmentMillis())
                        .setBackpressured(task.isBackpressured())
                        .setInputQueuedElements(task.inputQueuedElements())
                        .setInputCapacity(task.inputCapacity());
                long watermark = task.currentWatermark();
                if (watermark != Long.MIN_VALUE) {
                    status.setCurrentWatermark(watermark);
                }
                task.sourceLag().ifPresent(lag -> {
                    status.setSourceLagAvailable(true);
                    lag.partitions().forEach(partition -> status.addSourceLag(SourcePartitionLag.newBuilder()
                            .setTopic(partition.topic()).setPartition(partition.partition())
                            .setLagRecords(partition.lagRecords())));
                });
                builder.addTasks(status.build());
            }

            builder.setRecordsProcessed(totalRecords);
            stream.onNext(builder.build());
        } catch (Exception e) {
            // A failed beat is not worth failing the worker over. If the master really is gone,
            // it will stop hearing from this worker, which is the same signal either way.
            log.debug("a heartbeat could not be sent", e);
            beats = null;
            scheduleReconnect();
        }
    }

    private void handle(MasterCommand command) {
        switch (command.getCommandCase()) {
            case CANCEL_TASK -> {
                TaskId taskId = command.getCancelTask();
                log.info("master asked to cancel {}#{}",
                        taskId.getOperatorId(), taskId.getSubtaskIndex());
                taskManager.cancel(taskId.getOperatorId(), taskId.getSubtaskIndex());
            }
            case CANCEL_JOB -> {
                log.info("master asked to cancel job {}: {}",
                        command.getCancelJob().getJobId(), command.getCancelJob().getReason());
                taskManager.cancelAll();
            }
            case NO_OP, COMMAND_NOT_SET -> {
                // Nothing to do. The master acknowledges beats without always having something
                // to say.
            }
        }
    }

    @Override
    public void close() {
        closed = true;
        scheduler.shutdownNow();
        if (beats != null) {
            try {
                beats.onCompleted();
            } catch (RuntimeException e) {
                log.debug("closing the heartbeat stream failed", e);
            }
        }
    }
}
