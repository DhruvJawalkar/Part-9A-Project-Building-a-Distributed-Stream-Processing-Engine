package dev.dhruv.streaming.worker;

import dev.dhruv.streaming.api.ExchangeStrategy;
import dev.dhruv.streaming.api.Operator;
import dev.dhruv.streaming.api.Source;
import dev.dhruv.streaming.api.TimestampAssigner;
import dev.dhruv.streaming.rpc.ChainedOperator;
import dev.dhruv.streaming.rpc.InputChannel;
import dev.dhruv.streaming.rpc.OperatorKind;
import dev.dhruv.streaming.rpc.OutputChannel;
import dev.dhruv.streaming.rpc.TaskDeployment;
import dev.dhruv.streaming.runtime.OperatorChain;
import dev.dhruv.streaming.runtime.OperatorTask;
import dev.dhruv.streaming.runtime.RuntimeSourceContext;
import dev.dhruv.streaming.runtime.SerializationUtil;
import dev.dhruv.streaming.runtime.SourceTask;
import dev.dhruv.streaming.runtime.metrics.TaskMetricGroup;
import dev.dhruv.streaming.runtime.transport.DataTransportClient;
import dev.dhruv.streaming.runtime.transport.InputGate;
import dev.dhruv.streaming.runtime.transport.ResultPartitionWriter;
import dev.dhruv.streaming.runtime.transport.ResultSubpartition;
import dev.dhruv.streaming.runtime.transport.Subpartitions;
import dev.dhruv.streaming.runtime.transport.TaskInputRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

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

    private final Map<String, RunningTask> tasks = new ConcurrentHashMap<>();
    private BiConsumer<String, Throwable> failureListener = (taskId, failure) -> {
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
        this.workerId = workerId;
        this.host = host;
        this.dataPort = dataPort;
        this.inputRegistry = inputRegistry;
        this.transportClient = transportClient;
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
    public void onTaskFailure(BiConsumer<String, Throwable> listener) {
        this.failureListener = listener;
    }

    /**
     * Deploys one task and starts it.
     *
     * @param deployment everything the task needs
     * @return the task's key on this worker
     */
    public String deploy(TaskDeployment deployment) {
        String headOperatorId = deployment.getOperators(0).getOperatorId();
        int subtask = deployment.getSubtaskIndex();
        String taskKey = TaskInputRegistry.taskKey(headOperatorId, subtask);

        log.info("deploying {} ({}/{}) : {}", headOperatorId, subtask + 1,
                deployment.getParallelism(),
                deployment.getOperatorsList().stream().map(ChainedOperator::getOperatorId).toList());

        InputGate gate = buildInputGate(deployment);
        inputRegistry.register(headOperatorId, subtask, gate, senderChannels(deployment));

        ResultPartitionWriter output = buildOutput(deployment, headOperatorId, subtask);
        TaskMetricGroup metrics = new TaskMetricGroup(headOperatorId, subtask);

        Runnable task = deployment.getOperators(0).getKind() == OperatorKind.OPERATOR_SOURCE
                ? buildSourceTask(deployment, taskKey, output, metrics)
                : buildOperatorTask(deployment, taskKey, gate, output, metrics);

        Thread thread = new Thread(task, taskKey);
        tasks.put(taskKey, new RunningTask(taskKey, headOperatorId, subtask, task, thread,
                output, metrics));

        // Ship partly-filled buffers on a timer, so a trickle of records is not held hostage by
        // a batch that will never fill.
        bufferFlusher.scheduleAtFixedRate(() -> flushQuietly(output),
                DEFAULT_BUFFER_TIMEOUT_MILLIS, DEFAULT_BUFFER_TIMEOUT_MILLIS,
                TimeUnit.MILLISECONDS);

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

    private ResultPartitionWriter buildOutput(TaskDeployment deployment,
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

        ExchangeStrategy exchange = toApi(outputs.getFirst().getExchange());
        List<ResultSubpartition> subpartitions = new ArrayList<>(outputs.size());
        for (OutputChannel channel : outputs) {
            subpartitions.add(openChannel(senderTaskId, channel, exchange, subtaskIndex));
        }
        return new ResultPartitionWriter(subpartitions, exchange, subtaskIndex);
    }

    private ResultSubpartition openChannel(String senderTaskId,
                                           OutputChannel channel,
                                           ExchangeStrategy exchange,
                                           int senderSubtaskIndex) {
        // Which of the destination's input channels this sender occupies. Under forward the
        // destination has one input; otherwise every upstream subtask gets its own channel.
        int channelIndex = exchange == ExchangeStrategy.FORWARD ? 0 : senderSubtaskIndex;

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
                                     ResultPartitionWriter output,
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
                metrics);
        task.onFailure(failureListener);
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
                                       ResultPartitionWriter output,
                                       TaskMetricGroup metrics) {
        List<Operator<?, ?>> operators = new ArrayList<>();
        for (ChainedOperator operator : deployment.getOperatorsList()) {
            operators.add(SerializationUtil.fromBytes(
                    operator.getSerializedOperator().toByteArray()));
        }

        Operator<?, ?> operator = operators.size() == 1
                ? operators.getFirst()
                : new OperatorChain(operators);

        OperatorTask task = new OperatorTask(taskKey, operator, gate, output, metrics);
        task.onFailure(failureListener);
        return task;
    }

    /**
     * Cancels one task.
     *
     * @param operatorId   the operator
     * @param subtaskIndex which subtask
     */
    public void cancel(String operatorId, int subtaskIndex) {
        RunningTask task = tasks.remove(TaskInputRegistry.taskKey(operatorId, subtaskIndex));
        if (task == null) {
            return;
        }
        log.info("cancelling {}", task.taskKey());
        task.cancel();
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

    @Override
    public void close() {
        cancelAll();
        bufferFlusher.shutdownNow();
    }

    private static void flushQuietly(ResultPartitionWriter output) {
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
