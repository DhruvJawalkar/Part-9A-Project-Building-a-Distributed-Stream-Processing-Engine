package dev.dhruv.streaming.master;

import com.google.protobuf.ByteString;
import dev.dhruv.streaming.api.graph.JobGraph;
import dev.dhruv.streaming.api.graph.LogicalOperator;
import dev.dhruv.streaming.api.graph.SinkNode;
import dev.dhruv.streaming.api.graph.SourceNode;
import dev.dhruv.streaming.api.graph.TransformNode;
import dev.dhruv.streaming.master.graph.ChainBuilder;
import dev.dhruv.streaming.master.graph.ChainGroup;
import dev.dhruv.streaming.master.graph.ExecutionGraph;
import dev.dhruv.streaming.master.graph.TaskAssignment;
import dev.dhruv.streaming.metadata.RegisteredWorker;
import dev.dhruv.streaming.rpc.ChainedOperator;
import dev.dhruv.streaming.rpc.DeployAck;
import dev.dhruv.streaming.rpc.InputChannel;
import dev.dhruv.streaming.rpc.OperatorKind;
import dev.dhruv.streaming.rpc.OutputChannel;
import dev.dhruv.streaming.rpc.TaskDeployment;
import dev.dhruv.streaming.rpc.TaskId;
import dev.dhruv.streaming.rpc.CheckpointId;
import dev.dhruv.streaming.rpc.CheckpointTrigger;
import dev.dhruv.streaming.rpc.WorkerServiceGrpc;
import dev.dhruv.streaming.runtime.SerializationUtil;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Sends compiled tasks to the workers that will run them.
 *
 * <h2>Why deployment runs backwards</h2>
 *
 * <p>Tasks are deployed in <em>reverse</em> topological order: sinks first, sources last. The
 * reason is that a task starts running the moment it is deployed, and begins sending as soon as
 * it has something to send.
 *
 * <p>Deploy forwards and a source is producing records before the filter behind it exists. Those
 * records arrive at a worker that has never heard of their destination and are dropped, silently
 * and only sometimes -- the worst kind of bug, because it depends on how fast each RPC happened
 * to return. Deploying backwards makes it impossible: by the time anything can send, everything
 * it might send to is already listening.
 *
 * <p>This also removes the need for a separate "now start" round trip, which would otherwise be
 * the usual fix.
 */
public final class GrpcTaskDeployer implements TaskDeployer, AutoCloseable {

    // A worker gives external sink callbacks up to 90 seconds. The control-plane deadline must
    // be strictly larger so network delivery and response serialization cannot cancel a commit
    // at the worker's own boundary.
    private static final long CHECKPOINT_COMPLETION_RPC_TIMEOUT_SECONDS = 120;

    private static final Logger log = LoggerFactory.getLogger(GrpcTaskDeployer.class);

    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Map<String, RegisteredWorker> workersById = new ConcurrentHashMap<>();
    private final DeploymentRpc deploymentRpc;
    private final CancellationRpc cancellationRpc;

    public GrpcTaskDeployer() {
        this(null, null);
    }

    /** Package-private transport seam for testing uncertain RPC outcomes without a real socket. */
    GrpcTaskDeployer(DeploymentRpc deploymentRpc, CancellationRpc cancellationRpc) {
        this.deploymentRpc = deploymentRpc == null
                ? (worker, deployment) -> stubFor(worker)
                        .withDeadlineAfter(30, TimeUnit.SECONDS)
                        .deployTask(deployment)
                : deploymentRpc;
        this.cancellationRpc = cancellationRpc == null
                ? (worker, task) -> stubFor(worker)
                        .withDeadlineAfter(5, TimeUnit.SECONDS)
                        .cancelTask(task)
                : cancellationRpc;
    }

    @Override
    public void observeWorkers(List<RegisteredWorker> workers) {
        workers.forEach(worker -> workersById.put(worker.workerId(), worker));
    }

    @Override
    public void deploy(JobGraph graph, ExecutionGraph plan, List<RegisteredWorker> workers) {
        deploy(graph, plan, workers, Map.of());
    }

    @Override
    public void deployFromCheckpoint(JobGraph graph,
                                     ExecutionGraph plan,
                                     List<RegisteredWorker> workers,
                                     Map<String, String> stateHandles) {
        deploy(graph, plan, workers, stateHandles);
    }

    private void deploy(JobGraph graph,
                        ExecutionGraph plan,
                        List<RegisteredWorker> workers,
                        Map<String, String> stateHandles) {
        observeWorkers(workers);
        List<DeployedTask> attempted = new ArrayList<>();

        // Reverse topological order. See the class comment: this is what stops a running source
        // from sending to a task that has not been created yet.
        List<ChainGroup> reversed = new ArrayList<>(plan.chainGroups());
        java.util.Collections.reverse(reversed);

        try {
            for (ChainGroup chain : reversed) {
                for (int subtask = 0; subtask < chain.parallelism(); subtask++) {
                    TaskAssignment assignment =
                            plan.assignments().get(chain.id() + ":" + subtask);
                    RegisteredWorker worker = workersById.get(assignment.workerId());

                    TaskDeployment deployment =
                            buildDeployment(graph, plan, chain, subtask, worker, stateHandles);

                    // Once the request leaves the master its outcome is uncertain until proven
                    // otherwise. A deadline can fire after the worker has already started the
                    // task but before its ACK reaches us, so rollback must include this attempt.
                    attempted.add(new DeployedTask(worker, chain.head().id(), subtask));
                    DeployAck ack = deploymentRpc.deploy(worker, deployment);

                    if (!ack.getAccepted()) {
                        throw new IllegalStateException("worker " + worker.workerId()
                                + " refused " + chain.id() + ":" + subtask + " -- " + ack.getReason());
                    }
                    log.info("deployed {}:{} to {}", chain.id(), subtask, worker.workerId());
                }
            }
        } catch (RuntimeException deploymentFailure) {
            rollbackDeployment(graph.jobId(), attempted);
            throw deploymentFailure;
        }
    }

    /** Removes a partially installed generation before the restart policy makes another attempt. */
    private void rollbackDeployment(String jobId, List<DeployedTask> deployed) {
        for (int index = deployed.size() - 1; index >= 0; index--) {
            DeployedTask task = deployed.get(index);
            try {
                cancellationRpc.cancel(task.worker(), TaskId.newBuilder()
                        .setJobId(jobId)
                        .setOperatorId(task.operatorId())
                        .setSubtaskIndex(task.subtaskIndex())
                        .build());
            } catch (Exception rollbackFailure) {
                log.warn("could not roll back {}:{} on {}: {}", task.operatorId(),
                        task.subtaskIndex(), task.worker().workerId(), rollbackFailure.getMessage());
            }
        }
    }

    private record DeployedTask(RegisteredWorker worker, String operatorId, int subtaskIndex) {
    }

    @FunctionalInterface
    interface DeploymentRpc {
        DeployAck deploy(RegisteredWorker worker, TaskDeployment deployment);
    }

    @FunctionalInterface
    interface CancellationRpc {
        void cancel(RegisteredWorker worker, TaskId task);
    }

    private TaskDeployment buildDeployment(JobGraph graph,
                                           ExecutionGraph plan,
                                           ChainGroup chain,
                                           int subtaskIndex,
                                           RegisteredWorker worker,
                                           Map<String, String> stateHandles) {

        TaskDeployment.Builder deployment = TaskDeployment.newBuilder()
                .setJobId(graph.jobId())
                .setChainGroupId(chain.id())
                .setSubtaskIndex(subtaskIndex)
                .setParallelism(chain.parallelism());

        String stateHandle = stateHandles.get(chain.head().id() + ":" + subtaskIndex);
        if (stateHandle != null) {
            deployment.setStateHandleUri(stateHandle);
        }

        for (LogicalOperator operator : chain.operators()) {
            deployment.addOperators(serializeOperator(operator));
        }

        // Inputs: what feeds the head of this chain. The producing end of an upstream chain is
        // its tail, so that is the identity a receiver will see on arriving buffers.
        for (String upstreamId : chain.head().upstreamIds()) {
            ChainGroup upstream = chainEndingAt(plan, upstreamId);
            for (int upstreamSubtask = 0;
                 upstreamSubtask < upstream.parallelism();
                 upstreamSubtask++) {

                if (isForward(chain.head()) && upstreamSubtask != subtaskIndex) {
                    continue;          // forward: only subtask i feeds subtask i
                }
                deployment.addInputs(InputChannel.newBuilder()
                        .setUpstreamOperatorId(upstream.tail().id())
                        .setUpstreamSubtaskIndex(upstreamSubtask)
                        .setExchange(toWire(ChainBuilder.inputExchangeOf(chain.head())))
                        .build());
            }
        }

        // Outputs: where the tail of this chain sends. A downstream chain is addressed by its
        // head, which is what its worker registered it under.
        for (String downstreamId : graph.downstreamIdsOf(chain.tail().id())) {
            ChainGroup downstream = chainStartingAt(plan, downstreamId);
            for (int target = 0; target < downstream.parallelism(); target++) {
                RegisteredWorker targetWorker = workerFor(plan, downstream.id(), target);
                deployment.addOutputs(OutputChannel.newBuilder()
                        .setDownstreamOperatorId(downstream.head().id())
                        .setDownstreamSubtaskIndex(target)
                        .setExchange(toWire(ChainBuilder.inputExchangeOf(downstream.head())))
                        .setHost(targetWorker.host())
                        .setDataPort(targetWorker.dataPort())
                        .setLocal(targetWorker.workerId().equals(worker.workerId()))
                        .setSerializedKeySelector(bytesOrEmpty(keySelectorOf(downstream.head())))
                        .build());
            }
        }

        return deployment.build();
    }

    private static ChainedOperator serializeOperator(LogicalOperator operator) {
        ChainedOperator.Builder builder = ChainedOperator.newBuilder()
                .setOperatorId(operator.id());

        switch (operator) {
            case SourceNode source -> {
                builder.setKind(OperatorKind.OPERATOR_SOURCE);
                builder.setSerializedOperator(bytes(source.source()));
                builder.setSerializedTimestampAssigner(
                        bytesOrEmpty(source.timestampAssigner()));
                builder.setOutOfOrdernessMillis(source.outOfOrderness().toMillis());
                builder.setIdleTimeoutMillis(source.idleTimeout().toMillis());
            }
            case TransformNode transform -> {
                builder.setKind(OperatorKind.OPERATOR_TRANSFORM);
                builder.setSerializedOperator(bytes(transform.operator()));
                builder.setSerializedKeySelector(bytesOrEmpty(transform.keySelector()));
            }
            case SinkNode sink -> {
                builder.setKind(OperatorKind.OPERATOR_SINK);
                builder.setSerializedOperator(bytes(sink.operator()));
                builder.setSerializedKeySelector(bytesOrEmpty(sink.keySelector()));
            }
        }
        return builder.build();
    }

    @Override
    public void cancelAll(String jobId, ExecutionGraph plan) {
        // Best-effort by nature: this runs when something has already gone wrong, and some of
        // the workers being asked may be exactly the ones that stopped answering.
        plan.assignments().forEach((key, assignment) -> {
            RegisteredWorker worker = workersById.get(assignment.workerId());
            if (worker == null) {
                return;
            }
            ChainGroup chain = plan.chainGroup(assignment.chainGroupId()).orElseThrow();
            try {
                cancellationRpc.cancel(worker, TaskId.newBuilder()
                        .setJobId(jobId)
                        .setOperatorId(chain.head().id())
                        .setSubtaskIndex(assignment.subtaskIndex())
                        .build());
            } catch (Exception e) {
                log.warn("could not cancel {} on {}: {}",
                        key, worker.workerId(), e.getMessage());
            }
        });
    }

    private WorkerServiceGrpc.WorkerServiceBlockingStub stubFor(RegisteredWorker worker) {
        ManagedChannel channel = channels.computeIfAbsent(worker.workerId(), id ->
                NettyChannelBuilder.forAddress(worker.host(), worker.rpcPort())
                        .usePlaintext()
                        // A deployment carries the serialized user operators, which can be
                        // larger than gRPC's 4MB default once a job has a few of them.
                        .maxInboundMessageSize(64 * 1024 * 1024)
                        .build());
        return WorkerServiceGrpc.newBlockingStub(channel);
    }

    private RegisteredWorker workerFor(ExecutionGraph plan, String chainId, int subtask) {
        TaskAssignment assignment = plan.assignments().get(chainId + ":" + subtask);
        return workersById.get(assignment.workerId());
    }

    private static ChainGroup chainEndingAt(ExecutionGraph plan, String operatorId) {
        return plan.chainGroups().stream()
                .filter(chain -> chain.tail().id().equals(operatorId))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "no chain group ends at '" + operatorId + "'"));
    }

    private static ChainGroup chainStartingAt(ExecutionGraph plan, String operatorId) {
        return plan.chainGroups().stream()
                .filter(chain -> chain.head().id().equals(operatorId))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "no chain group starts at '" + operatorId + "'"));
    }

    private static boolean isForward(LogicalOperator operator) {
        return ChainBuilder.inputExchangeOf(operator)
                == dev.dhruv.streaming.api.ExchangeStrategy.FORWARD;
    }

    @Override
    public void triggerSources(JobGraph graph, ExecutionGraph plan,
                               long checkpointId, long triggerTimestamp) {
        // TaskManager injects into every matching source it hosts. One RPC per worker is
        // therefore deliberate: calling once per source subtask would inject duplicate barriers
        // when a worker owns more than one source partition.
        java.util.Set<RegisteredWorker> sourceWorkers = new LinkedHashSet<>();
        for (ChainGroup chain : plan.chainGroups()) {
            if (!(chain.head() instanceof SourceNode)) {
                continue;
            }
            for (int subtask = 0; subtask < chain.parallelism(); subtask++) {
                sourceWorkers.add(workerFor(plan, chain.id(), subtask));
            }
        }
        sourceWorkers.forEach(worker -> stubFor(worker).withDeadlineAfter(5, TimeUnit.SECONDS)
                .triggerCheckpoint(CheckpointTrigger.newBuilder()
                        .setCheckpointId(checkpointId)
                        .setTriggerTs(triggerTimestamp)
                        .setJobId(graph.jobId())
                        .build()));
    }

    @Override
    public void notifySinks(JobGraph graph, ExecutionGraph plan, long checkpointId) {
        // TaskManager fans one completion message out to every sink task for this job on the
        // worker. Contact each worker once: sending once per sink subtask would replay the same
        // notification many times when a worker hosts several sink tasks.
        java.util.Set<RegisteredWorker> sinkWorkers = new LinkedHashSet<>();
        for (ChainGroup chain : plan.chainGroups()) {
            if (!(chain.tail() instanceof SinkNode)) {
                continue;
            }
            for (int subtask = 0; subtask < chain.parallelism(); subtask++) {
                sinkWorkers.add(workerFor(plan, chain.id(), subtask));
            }
        }
        CheckpointId completion = CheckpointId.newBuilder()
                .setCheckpointId(checkpointId)
                .setJobId(graph.jobId())
                .build();
        sinkWorkers.forEach(worker -> stubFor(worker)
                .withDeadlineAfter(CHECKPOINT_COMPLETION_RPC_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .notifyCheckpointComplete(completion));
    }

    @Override
    public void abortCheckpoint(JobGraph graph, ExecutionGraph plan, long checkpointId) {
        java.util.Set<RegisteredWorker> jobWorkers = new LinkedHashSet<>();
        plan.workerIds().forEach(workerId -> {
            RegisteredWorker worker = workersById.get(workerId);
            if (worker != null) {
                jobWorkers.add(worker);
            }
        });
        jobWorkers.forEach(worker -> {
            try {
                stubFor(worker).withDeadlineAfter(5, TimeUnit.SECONDS)
                        .abortCheckpoint(CheckpointId.newBuilder()
                                .setCheckpointId(checkpointId)
                                .setJobId(graph.jobId())
                                .build());
            } catch (RuntimeException failure) {
                log.warn("could not abort checkpoint {} on {}: {}", checkpointId,
                        worker.workerId(), failure.getMessage());
            }
        });
    }

    private static Optional<dev.dhruv.streaming.api.KeySelector<?, ?>> keySelectorOf(
            LogicalOperator operator) {
        return switch (operator) {
            case TransformNode transform -> transform.keySelector();
            case SinkNode sink -> sink.keySelector();
            case SourceNode ignored -> Optional.empty();
        };
    }

    private static ByteString bytes(Serializable value) {
        return ByteString.copyFrom(SerializationUtil.toBytes(value));
    }

    private static ByteString bytesOrEmpty(Optional<? extends Serializable> value) {
        return ByteString.copyFrom(SerializationUtil.toBytesOrEmpty(value));
    }

    private static dev.dhruv.streaming.rpc.ExchangeStrategy toWire(
            dev.dhruv.streaming.api.ExchangeStrategy strategy) {
        return switch (strategy) {
            case FORWARD -> dev.dhruv.streaming.rpc.ExchangeStrategy.EXCHANGE_FORWARD;
            case HASH -> dev.dhruv.streaming.rpc.ExchangeStrategy.EXCHANGE_HASH;
            case BROADCAST -> dev.dhruv.streaming.rpc.ExchangeStrategy.EXCHANGE_BROADCAST;
            case REBALANCE -> dev.dhruv.streaming.rpc.ExchangeStrategy.EXCHANGE_REBALANCE;
        };
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
