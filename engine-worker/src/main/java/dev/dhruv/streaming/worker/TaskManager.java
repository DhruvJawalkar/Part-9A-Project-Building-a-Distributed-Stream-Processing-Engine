package dev.dhruv.streaming.worker;

import dev.dhruv.streaming.api.ExchangeStrategy;
import dev.dhruv.streaming.api.CheckpointBarrier;
import dev.dhruv.streaming.api.Operator;
import dev.dhruv.streaming.api.KeySelector;
import dev.dhruv.streaming.api.Source;
import dev.dhruv.streaming.api.TimestampAssigner;
import dev.dhruv.streaming.api.state.StateHandle;
import dev.dhruv.streaming.rpc.ChainedOperator;
import dev.dhruv.streaming.rpc.InputChannel;
import dev.dhruv.streaming.rpc.OperatorKind;
import dev.dhruv.streaming.rpc.OutputChannel;
import dev.dhruv.streaming.rpc.TaskDeployment;
import dev.dhruv.streaming.rpc.CheckpointAck;
import dev.dhruv.streaming.rpc.TaskId;
import dev.dhruv.streaming.rpc.CheckpointTrigger;
import dev.dhruv.streaming.runtime.OperatorChain;
import dev.dhruv.streaming.runtime.OperatorTask;
import dev.dhruv.streaming.runtime.RuntimeSourceContext;
import dev.dhruv.streaming.runtime.RocksDbStateBackend;
import dev.dhruv.streaming.runtime.SerializationUtil;
import dev.dhruv.streaming.runtime.SourceTask;
import dev.dhruv.streaming.runtime.metrics.TaskMetricGroup;
import dev.dhruv.streaming.runtime.transport.DataTransportClient;
import dev.dhruv.streaming.runtime.transport.FanOutOutput;
import dev.dhruv.streaming.runtime.transport.InputGate;
import dev.dhruv.streaming.runtime.transport.Output;
import dev.dhruv.streaming.runtime.transport.ResultPartitionWriter;
import dev.dhruv.streaming.runtime.transport.ResultSubpartition;
import dev.dhruv.streaming.runtime.transport.Subpartitions;
import dev.dhruv.streaming.runtime.transport.TaskInputRegistry;
import dev.dhruv.streaming.worker.checkpoint.CheckpointStorage;
import dev.dhruv.streaming.worker.checkpoint.FileSystemCheckpointStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Base64;
import java.util.UUID;
import java.nio.file.Path;
import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Turns a {@code TaskDeployment} into running threads on this worker.
 *
 * <p>This is where the master's plan stops being a description and starts being a process doing
 * work. Everything the deployment carries is assembled here: the user's operators are
 * deserialized, an input gate is built with one channel per upstream subtask, outgoing channels
 * are opened to wherever the downstream subtasks turned out to be, and a thread is started.
 *
 * <p>The one decision made locally rather than by the master is whether each outgoing channel is
 * a queue handoff or a network stream, and even that is only read off the {@code local} flag the
 * master already set.
 */
public final class TaskManager implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TaskManager.class);

    /**
     * How often batched records are shipped even if their buffer has not filled.
     *
     * <p>The article's latency-versus-throughput dial. Lower it and records travel sooner in
     * smaller batches; raise it and they travel together, which costs less per record and makes
     * each record wait longer. 100ms is a default, not a discovery.
     */
    public static final long DEFAULT_BUFFER_TIMEOUT_MILLIS = 100;

    private final String workerId;
    private final String host;
    private final int dataPort;
    private final TaskInputRegistry inputRegistry;
    private final DataTransportClient transportClient;
    private final ScheduledExecutorService bufferFlusher;
    private final Path checkpointRoot;
    private final CheckpointStorage checkpointStorage;

    private final Map<String, RunningTask> tasks = new ConcurrentHashMap<>();
    private TaskFailureListener failureListener = (jobId, taskId, failure) -> {
    };
    private java.util.function.Consumer<CheckpointAck> checkpointAcknowledgement = acknowledgement -> {
    };

    /**
     * Creates a task manager.
     *
     * @param workerId        this worker's id, for log lines
     * @param host            the address peers should reach this worker on
     * @param dataPort        the data-plane port peers should connect to
     * @param inputRegistry   where deployed tasks are registered so arriving buffers find them
     * @param transportClient used to open outgoing streams to peer workers
     */
    public TaskManager(String workerId,
                       String host,
                       int dataPort,
                       TaskInputRegistry inputRegistry,
                       DataTransportClient transportClient) {
        this(workerId, host, dataPort, inputRegistry, transportClient,
                Path.of(System.getProperty("java.io.tmpdir"), "distributed-stream-processing-engine",
                        "checkpoints"));
    }

    /**
     * Creates a task manager with an explicit durable checkpoint root.
     *
     * @param checkpointRoot directory shared by the worker processes that may restore this job
     */
    public TaskManager(String workerId,
                       String host,
                       int dataPort,
                       TaskInputRegistry inputRegistry,
                       DataTransportClient transportClient,
                       Path checkpointRoot) {
        this(workerId, host, dataPort, inputRegistry, transportClient, checkpointRoot,
                new FileSystemCheckpointStorage(checkpointRoot.resolve("archives")));
    }

    /**
     * Creates a task manager with explicit local staging and durable checkpoint storage.
     * The separate locations are intentional: staging is worker-local, the storage handle is not.
     */
    public TaskManager(String workerId,
                       String host,
                       int dataPort,
                       TaskInputRegistry inputRegistry,
                       DataTransportClient transportClient,
                       Path checkpointRoot,
                       CheckpointStorage checkpointStorage) {
        this.workerId = workerId;
        this.host = host;
        this.dataPort = dataPort;
        this.inputRegistry = inputRegistry;
        this.transportClient = transportClient;
        this.checkpointRoot = checkpointRoot.toAbsolutePath();
        this.checkpointStorage = checkpointStorage;
        this.bufferFlusher = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "buffer-flusher");
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * Registers what to do when a task on this worker fails.
     *
     * @param listener called with the task id and the failure
     */
    public void onTaskFailure(TaskFailureListener listener) {
        this.failureListener = listener;
    }

    /** Registers the control-plane callback for task checkpoint acknowledgements. */
    public void onCheckpointAcknowledgement(java.util.function.Consumer<CheckpointAck> listener) {
        this.checkpointAcknowledgement = listener;
    }

    /**
     * Deploys one task and starts it.
     *
     * @param deployment everything the task needs
     * @return the task's key on this worker
     */
    public synchronized String deploy(TaskDeployment deployment) {
        String headOperatorId = deployment.getOperators(0).getOperatorId();
        int subtask = deployment.getSubtaskIndex();
        String taskKey = TaskInputRegistry.taskKey(headOperatorId, subtask);
        if (tasks.containsKey(taskKey)) {
            throw new IllegalStateException("task " + taskKey + " is still running");
        }

        log.info("deploying {} ({}/{}) : {}", headOperatorId, subtask + 1,
                deployment.getParallelism(),
                deployment.getOperatorsList().stream().map(ChainedOperator::getOperatorId).toList());

        InputGate gate = buildInputGate(deployment);
        inputRegistry.register(headOperatorId, subtask, gate, senderChannels(deployment));

        Output output = buildOutput(deployment, headOperatorId, subtask);
        TaskMetricGroup metrics = new TaskMetricGroup(headOperatorId, subtask);

        Runnable task = deployment.getOperators(0).getKind() == OperatorKind.OPERATOR_SOURCE
                ? buildSourceTask(deployment, taskKey, output, metrics)
                : buildOperatorTask(deployment, taskKey, gate, output, metrics);

        // Ship partly-filled buffers on a timer, so a trickle of records is not held hostage by
        // a batch that will never fill.
        ScheduledFuture<?> flushTask = bufferFlusher.scheduleAtFixedRate(() -> flushQuietly(output),
                DEFAULT_BUFFER_TIMEOUT_MILLIS, DEFAULT_BUFFER_TIMEOUT_MILLIS,
                TimeUnit.MILLISECONDS);

        Thread thread = new Thread(task, taskKey);
        tasks.put(taskKey, new RunningTask(taskKey, deployment.getJobId(), headOperatorId, subtask,
                task, thread, output, flushTask, metrics));

        thread.start();
        return taskKey;
    }

    private InputGate buildInputGate(TaskDeployment deployment) {
        int channels = Math.max(deployment.getInputsCount(), 1);
        return new InputGate(channels, InputGate.DEFAULT_CHANNEL_CAPACITY, channel -> {
            // Phase 2 grants credit from the receiving gRPC handler rather than from here; see
            // DataTransportService. This hook is where a more precise scheme would report each
            // consumed element back to its sender.
        });
    }

    /**
     * Maps each upstream subtask's wire identity to the channel index it occupies at this gate.
     *
     * <p>Order matters and is the master's: the deployment lists inputs in a fixed order, and
     * channel {@code i} is the {@code i}th of them. Both ends agree because both read the same
     * list.
     */
    private Map<String, Integer> senderChannels(TaskDeployment deployment) {
        Map<String, Integer> channels = new LinkedHashMap<>();
        List<InputChannel> inputs = deployment.getInputsList();
        for (int i = 0; i < inputs.size(); i++) {
            InputChannel input = inputs.get(i);
            channels.put(
                    TaskInputRegistry.taskKey(
                            input.getUpstreamOperatorId(), input.getUpstreamSubtaskIndex()),
                    i);
        }
        return channels;
    }

    private Output buildOutput(TaskDeployment deployment,
                               String headOperatorId,
                               int subtaskIndex) {
        List<OutputChannel> outputs = deployment.getOutputsList();
        if (outputs.isEmpty()) {
            return new ResultPartitionWriter(
                    List.of(), ExchangeStrategy.FORWARD, subtaskIndex);
        }

        // The tail operator is what actually emits, so it is the tail's identity a receiver sees
        // as the sender. A chained group's head is how the master addresses it; its tail is how
        // downstream tasks know it.
        String tailOperatorId = deployment.getOperators(
                deployment.getOperatorsCount() - 1).getOperatorId();
        String senderTaskId = TaskInputRegistry.taskKey(tailOperatorId, subtaskIndex);

        // A deployment lists one channel per downstream subtask. Keep all channels for one
        // downstream operator together, but do not flatten different edges into one writer:
        // their exchanges and key selectors are independent.
        Map<String, List<OutputChannel>> channelsByDownstream = new LinkedHashMap<>();
        for (OutputChannel channel : outputs) {
            channelsByDownstream.computeIfAbsent(channel.getDownstreamOperatorId(),
                    ignored -> new ArrayList<>()).add(channel);
        }

        List<Output> edgeOutputs = new ArrayList<>(channelsByDownstream.size());
        for (List<OutputChannel> edgeChannels : channelsByDownstream.values()) {
            OutputChannel edge = edgeChannels.getFirst();
            ExchangeStrategy exchange = toApi(edge.getExchange());
            Optional<KeySelector<?, ?>> keySelector = SerializationUtil.fromBytesOrEmpty(
                    edge.getSerializedKeySelector().toByteArray());
            List<ResultSubpartition> subpartitions = new ArrayList<>(edgeChannels.size());
            for (OutputChannel channel : edgeChannels) {
                subpartitions.add(openChannel(senderTaskId, channel));
            }
            edgeOutputs.add(new ResultPartitionWriter(subpartitions, exchange, subtaskIndex,
                    keySelector.orElse(null)));
        }
        return edgeOutputs.size() == 1 ? edgeOutputs.getFirst() : new FanOutOutput(edgeOutputs);
    }

    private ResultSubpartition openChannel(String senderTaskId,
                                           OutputChannel channel) {

        if (channel.getLocal()) {
            // Same worker: hand straight to the destination's gate. Registered already, because
            // the master deploys downstream tasks before the tasks that feed them.
            var destination = inputRegistry.resolve(
                    channel.getDownstreamOperatorId(),
                    channel.getDownstreamSubtaskIndex(),
                    senderTaskId);
            if (destination.isPresent()) {
                return Subpartitions.local(
                        destination.get().channelIndex(), destination.get().gate());
            }
            log.warn("expected {}#{} to be deployed here already; falling back to the network",
                    channel.getDownstreamOperatorId(), channel.getDownstreamSubtaskIndex());
        }

        return transportClient.openSubpartition(
                senderTaskId,
                channel.getDownstreamOperatorId(),
                channel.getDownstreamSubtaskIndex(),
                channel.getHost().isEmpty() ? host : channel.getHost(),
                channel.getDataPort() == 0 ? dataPort : channel.getDataPort());
    }

    private Runnable buildSourceTask(TaskDeployment deployment,
                                     String taskKey,
                                     Output output,
                                     TaskMetricGroup metrics) {
        ChainedOperator head = deployment.getOperators(0);
        Source<?> source = SerializationUtil.fromBytes(head.getSerializedOperator().toByteArray());
        Optional<TimestampAssigner<?>> assigner = SerializationUtil.fromBytesOrEmpty(
                head.getSerializedTimestampAssigner().toByteArray());

        // A source chained with the operators behind it: the source emits into them directly,
        // on the same thread, with no queue in between.
        Optional<Operator<?, ?>> chained = chainBehindSource(deployment);

        SourceTask task = new SourceTask(
                taskKey,
                source,
                new RuntimeSourceContext(
                        deployment.getSubtaskIndex(), deployment.getParallelism(), metrics),
                chained.isPresent()
                        ? new ChainedSourceOutput(chained.get(), output, metrics)
                        : output,
                assigner,
                head.getOutOfOrdernessMillis(),
                head.getIdleTimeoutMillis(),
                metrics);
        if (!deployment.getStateHandleUri().isBlank()) {
            task.restore(materializeRestoreHandle(deployment, taskKey));
        }
        task.onCheckpoint(result -> result.stateHandle().ifPresent(handle ->
                acknowledgeCheckpoint(deployment, taskKey, result.checkpointId(), handle,
                        result.alignmentMillis())));
        task.onFailure((ignored, failure) ->
                failureListener.failed(deployment.getJobId(), taskKey, failure));
        return task;
    }

    private Optional<Operator<?, ?>> chainBehindSource(TaskDeployment deployment) {
        if (deployment.getOperatorsCount() == 1) {
            return Optional.empty();
        }
        List<Operator<?, ?>> operators = new ArrayList<>();
        for (int i = 1; i < deployment.getOperatorsCount(); i++) {
            operators.add(SerializationUtil.fromBytes(
                    deployment.getOperators(i).getSerializedOperator().toByteArray()));
        }
        return Optional.of(operators.size() == 1 ? operators.getFirst()
                : new OperatorChain(operators));
    }

    private Runnable buildOperatorTask(TaskDeployment deployment,
                                       String taskKey,
                                       InputGate gate,
                                       Output output,
                                       TaskMetricGroup metrics) {
        List<Operator<?, ?>> operators = new ArrayList<>();
        for (ChainedOperator operator : deployment.getOperatorsList()) {
            operators.add(SerializationUtil.fromBytes(
                    operator.getSerializedOperator().toByteArray()));
        }

        Operator<?, ?> operator = operators.size() == 1
                ? operators.getFirst()
                : new OperatorChain(operators);

        Optional<KeySelector<?, ?>> keySelector = SerializationUtil.fromBytesOrEmpty(
                deployment.getOperators(0).getSerializedKeySelector().toByteArray());

        Path taskRoot = checkpointRoot
                .resolve(deployment.getJobId())
                .resolve(taskKey.replace('#', '_'));
        RocksDbStateBackend stateBackend;
        try {
            stateBackend = new RocksDbStateBackend(taskRoot.resolve("live-rocksdb"));
        } catch (java.io.IOException failure) {
            throw new IllegalStateException("could not open RocksDB for " + taskKey, failure);
        }

        OperatorTask task = new OperatorTask(taskKey, operator, gate, output, keySelector,
                stateBackend, taskRoot.resolve("completed"), metrics);
        if (!deployment.getStateHandleUri().isBlank()) {
            task.restore(materializeRestoreHandle(deployment, taskKey));
        }
        ChainedOperator head = deployment.getOperators(0);
        task.onCheckpoint(result -> acknowledgeCheckpoint(deployment, taskKey, result.checkpointId(),
                result.stateHandle(), result.alignmentMillis()));
        task.onFailure((ignored, failure) ->
                failureListener.failed(deployment.getJobId(), taskKey, failure));
        return task;
    }

    /**
     * Cancels one task.
     *
     * @param operatorId   the operator
     * @param subtaskIndex which subtask
     */
    public synchronized void cancel(String operatorId, int subtaskIndex) {
        String taskKey = TaskInputRegistry.taskKey(operatorId, subtaskIndex);
        RunningTask task = tasks.get(taskKey);
        if (task == null) {
            return;
        }
        log.info("cancelling {}", task.taskKey());
        task.cancel();
        try {
            task.thread().join(2_000);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting for " + taskKey
                    + " to stop", interrupted);
        }
        if (task.isAlive()) {
            // Do not remove or replace it. Deploying a restored copy under the same identity
            // while this thread can still emit would create two histories after one checkpoint.
            throw new IllegalStateException("task " + taskKey
                    + " did not stop before recovery redeployment");
        }
        tasks.remove(taskKey, task);
        inputRegistry.unregister(operatorId, subtaskIndex);
    }

    /**
     * Cancels every task of a job. Phase 2 runs one job at a time, so this is all of them.
     */
    public void cancelAll() {
        List.copyOf(tasks.values()).forEach(task -> cancel(task.operatorId(), task.subtaskIndex()));
    }

    /**
     * Returns the running tasks, for the heartbeat.
     *
     * @return the tasks, by key
     */
    public Map<String, RunningTask> runningTasks() {
        return Map.copyOf(tasks);
    }

    /**
     * Injects a checkpoint barrier into every source currently hosted by this worker.
     *
     * <p>Only source tasks receive a control-plane trigger. Every other task learns of the
     * checkpoint through its ordered input channels, which is the consistency property that
     * barrier alignment preserves.
     */
    public void triggerCheckpoint(CheckpointTrigger trigger) throws Exception {
        CheckpointBarrier barrier = new CheckpointBarrier(trigger.getCheckpointId(), trigger.getTriggerTs());
        for (RunningTask task : List.copyOf(tasks.values())) {
            if (task.jobId().equals(trigger.getJobId()) && task.task() instanceof SourceTask source) {
                Path taskCheckpointDirectory = checkpointRoot
                        .resolve(trigger.getJobId())
                        .resolve(task.taskKey().replace('#', '_'))
                        .resolve("checkpoint-" + trigger.getCheckpointId());
                source.triggerCheckpoint(barrier, taskCheckpointDirectory);
            }
        }
    }

    /** Releases operator channels held by an abandoned checkpoint for one job. */
    public void abortCheckpoint(String jobId, long checkpointId) {
        for (RunningTask task : List.copyOf(tasks.values())) {
            if (task.jobId().equals(jobId) && task.task() instanceof OperatorTask operator) {
                operator.abortCheckpoint(checkpointId);
            }
        }
    }

    /** Publishes a fully written local task snapshot before its acknowledgement reaches the master. */
    private void acknowledgeCheckpoint(TaskDeployment deployment,
                                       String taskKey,
                                       long checkpointId,
                                       StateHandle localHandle,
                                       long alignmentMillis) {
        try {
            Path entryPoint = Path.of(localHandle.uri()).toAbsolutePath().normalize();
            Path checkpointDirectory = entryPoint.getParent();
            if (checkpointDirectory == null) {
                throw new java.io.IOException("checkpoint entry point has no parent: " + entryPoint);
            }
            StateHandle durableHandle = checkpointStorage.publish(
                    checkpointObjectKey(deployment.getJobId(), taskKey, checkpointId),
                    checkpointDirectory, localHandle);
            checkpointAcknowledgement.accept(CheckpointAck.newBuilder()
                    .setWorkerId(workerId)
                    .setTaskId(TaskId.newBuilder()
                            .setJobId(deployment.getJobId())
                            .setOperatorId(deployment.getOperators(0).getOperatorId())
                            .setSubtaskIndex(deployment.getSubtaskIndex()))
                    .setCheckpointId(checkpointId)
                    .setStateHandleUri(durableHandle.uri().toString())
                    .setStateSizeBytes(durableHandle.sizeBytes())
                    .setAlignmentMillis(alignmentMillis)
                    .build());
        } catch (Exception failure) {
            throw new CheckpointPublicationException("could not publish checkpoint " + checkpointId
                    + " for " + taskKey, failure);
        }
    }

    /** Downloads an immutable archive to this worker before the task's local restore begins. */
    private StateHandle materializeRestoreHandle(TaskDeployment deployment, String taskKey) {
        try {
            Path target = checkpointRoot.resolve("restored")
                    .resolve(encodedComponent(deployment.getJobId()))
                    .resolve(encodedComponent(taskKey))
                    .resolve(UUID.randomUUID().toString());
            return checkpointStorage.materialize(new StateHandle(
                    URI.create(deployment.getStateHandleUri()), 0), target);
        } catch (Exception failure) {
            throw new IllegalStateException("could not materialize checkpoint for " + taskKey, failure);
        }
    }

    private static String checkpointObjectKey(String jobId, String taskKey, long checkpointId) {
        return "jobs/" + encodedComponent(jobId) + "/tasks/" + encodedComponent(taskKey)
                + "/checkpoint-" + checkpointId + ".zip";
    }

    private static String encodedComponent(String value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(
                value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static final class CheckpointPublicationException extends RuntimeException {
        private CheckpointPublicationException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /** Carries the job identity that a task-local id alone cannot provide. */
    @FunctionalInterface
    public interface TaskFailureListener {
        void failed(String jobId, String taskId, Throwable failure);
    }

    @Override
    public void close() {
        cancelAll();
        bufferFlusher.shutdownNow();
    }

    private static void flushQuietly(Output output) {
        try {
            output.flush();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            // The task is failing or gone; its own thread will report it.
            log.debug("a scheduled flush failed", e);
        }
    }

    private static ExchangeStrategy toApi(dev.dhruv.streaming.rpc.ExchangeStrategy wire) {
        return switch (wire) {
            case EXCHANGE_FORWARD -> ExchangeStrategy.FORWARD;
            case EXCHANGE_HASH -> ExchangeStrategy.HASH;
            case EXCHANGE_BROADCAST -> ExchangeStrategy.BROADCAST;
            case EXCHANGE_REBALANCE -> ExchangeStrategy.REBALANCE;
            case EXCHANGE_UNSPECIFIED, UNRECOGNIZED -> throw new IllegalArgumentException(
                    "a deployment arrived with no exchange strategy set");
        };
    }
}
