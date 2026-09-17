package dev.dhruv.streaming.runtime;

import dev.dhruv.streaming.api.ExchangeStrategy;
import dev.dhruv.streaming.api.JobExecutor;
import dev.dhruv.streaming.api.graph.JobGraph;
import dev.dhruv.streaming.api.graph.LogicalOperator;
import dev.dhruv.streaming.api.graph.SinkNode;
import dev.dhruv.streaming.api.graph.SourceNode;
import dev.dhruv.streaming.api.graph.TransformNode;
import dev.dhruv.streaming.runtime.metrics.TaskMetricGroup;
import dev.dhruv.streaming.runtime.transport.InputGate;
import dev.dhruv.streaming.runtime.transport.Output;
import dev.dhruv.streaming.runtime.transport.ResultPartitionWriter;
import dev.dhruv.streaming.runtime.transport.ResultSubpartition;
import dev.dhruv.streaming.runtime.transport.Subpartitions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Runs a whole job inside one JVM: one thread per subtask, input gates between them.
 *
 * <p>Phase 1's engine, still useful. There is no master, no scheduler and no network -- the
 * graph is walked in topological order, a gate is created for every subtask, and each task is
 * handed the gates belonging to whatever comes next.
 *
 * <p>It is worth being clear about what this is and is not. It is a genuine implementation of
 * task execution, backpressure and record routing, and the {@link OperatorTask} it starts is the
 * same class a worker runs in Phase 2, constructed the same way. It is not distribution: every
 * task is in this process, so nothing crosses a network, nothing can partially fail, and no task
 * can outlive another.
 *
 * <p>What it is for from here on is running a job without a cluster -- for tests, for demos, and
 * for reading. A determinism test comparing two runs of the same fixture does not become more
 * convincing by involving three worker processes, and it becomes considerably harder to debug.
 */
public final class LocalJobExecutor implements JobExecutor {

    private static final Logger log = LoggerFactory.getLogger(LocalJobExecutor.class);

    private final JobGraph graph;
    private final List<Thread> threads = new ArrayList<>();
    private final List<SourceTask> sourceTasks = new ArrayList<>();
    private final List<OperatorTask> operatorTasks = new ArrayList<>();

    /** Input gates, one list of subtask gates per operator id. */
    private final Map<String, List<InputGate>> inputGates = new HashMap<>();

    /**
     * Creates an executor for a job.
     *
     * @param graph the validated job graph
     */
    public LocalJobExecutor(JobGraph graph) {
        this.graph = graph;
    }

    /**
     * Builds every task and starts it.
     *
     * <p>Gates are created for every operator first, so that a task's downstream gates already
     * exist by the time it needs to be handed them.
     */
    @Override
    public void start() {
        log.info("starting job '{}' ({}) with {} operators",
                graph.name(), graph.jobId(), graph.operators().size());

        for (LogicalOperator operator : graph.operatorsInTopologicalOrder()) {
            inputGates.put(operator.id(), createGates(operator));
        }

        for (LogicalOperator operator : graph.operatorsInTopologicalOrder()) {
            switch (operator) {
                case SourceNode source -> startSourceSubtasks(source);
                case TransformNode transform -> startOperatorSubtasks(
                        transform, transform.inputExchange());
                case SinkNode sink -> startOperatorSubtasks(sink, sink.inputExchange());
            }
        }

        threads.forEach(Thread::start);
        log.info("job '{}' running with {} tasks", graph.name(), threads.size());
    }

    private List<InputGate> createGates(LogicalOperator operator) {
        int channels = Math.max(upstreamSubtasksFeeding(operator), 1);
        List<InputGate> gates = new ArrayList<>(operator.parallelism());
        for (int i = 0; i < operator.parallelism(); i++) {
            gates.add(new InputGate(channels));
        }
        return gates;
    }

    private void startSourceSubtasks(SourceNode source) {
        for (int subtask = 0; subtask < source.parallelism(); subtask++) {
            String taskId = taskId(source, subtask);
            TaskMetricGroup metrics = new TaskMetricGroup(source.id(), subtask);
            SourceTask task = new SourceTask(
                    taskId,
                    // A private copy per subtask. Sharing one instance across four threads is
                    // the bug this prevents; see TaskInstances.
                    TaskInstances.copyOf(source.source()),
                    new RuntimeSourceContext(subtask, source.parallelism(), metrics),
                    outputFor(source, subtask),
                    source.timestampAssigner(),
                    metrics);

            sourceTasks.add(task);
            threads.add(new Thread(task, taskId));
        }
    }

    private void startOperatorSubtasks(LogicalOperator node, ExchangeStrategy inputExchange) {
        if (inputExchange == ExchangeStrategy.HASH) {
            throw new UnsupportedOperationException(
                    "operator '" + node.id() + "' needs a hash exchange, which arrives in"
                            + " Phase 3 along with keyed state. Phase 2 runs forward and"
                            + " rebalance edges only.");
        }

        List<InputGate> gates = inputGates.get(node.id());
        for (int subtask = 0; subtask < node.parallelism(); subtask++) {
            String taskId = taskId(node, subtask);
            TaskMetricGroup metrics = new TaskMetricGroup(node.id(), subtask);
            OperatorTask task = new OperatorTask(
                    taskId,
                    TaskInstances.copyOf(operatorOf(node)),
                    gates.get(subtask),
                    outputFor(node, subtask),
                    metrics);

            operatorTasks.add(task);
            threads.add(new Thread(task, taskId));
        }
    }

    /**
     * Returns how many upstream subtasks feed each subtask of this node.
     *
     * <p>Under a forward exchange the answer is one, because subtask {@code i} hears only from
     * subtask {@code i}. Under any redistributing exchange every upstream subtask may send to
     * every downstream one, so the answer is all of them.
     */
    private int upstreamSubtasksFeeding(LogicalOperator node) {
        if (node instanceof SourceNode) {
            return 0;
        }
        if (inputExchangeOf(node) == ExchangeStrategy.FORWARD) {
            return 1;
        }
        return node.upstreamIds().stream()
                .map(id -> graph.operator(id).orElseThrow())
                .mapToInt(LogicalOperator::parallelism)
                .sum();
    }

    /**
     * Builds the output for one subtask: the gates of everything downstream of it.
     */
    private Output outputFor(LogicalOperator node, int subtaskIndex) {
        List<String> downstreamIds = graph.downstreamIdsOf(node.id());
        if (downstreamIds.isEmpty()) {
            return new ResultPartitionWriter(List.of(), ExchangeStrategy.FORWARD, subtaskIndex);
        }
        if (downstreamIds.size() > 1) {
            throw new UnsupportedOperationException(
                    "operator '" + node.id() + "' fans out to " + downstreamIds
                            + "; the local executor runs linear pipelines only");
        }

        String downstreamId = downstreamIds.getFirst();
        LogicalOperator downstream = graph.operator(downstreamId).orElseThrow();
        ExchangeStrategy exchange = inputExchangeOf(downstream);

        // Which input channel this sender occupies at the receiver. Under forward the receiver
        // has exactly one input, so channel zero. Otherwise every upstream subtask gets its own
        // channel, which is what lets the receiver tell them apart -- and, from Phase 4, block
        // them independently.
        List<ResultSubpartition> subpartitions = new ArrayList<>();
        List<InputGate> downstreamGates = inputGates.get(downstreamId);
        int channelIndex = exchange == ExchangeStrategy.FORWARD ? 0 : subtaskIndex;
        for (int target = 0; target < downstream.parallelism(); target++) {
            subpartitions.add(Subpartitions.local(channelIndex, downstreamGates.get(target)));
        }
        return new ResultPartitionWriter(subpartitions, exchange, subtaskIndex);
    }

    private static ExchangeStrategy inputExchangeOf(LogicalOperator node) {
        return switch (node) {
            case TransformNode transform -> transform.inputExchange();
            case SinkNode sink -> sink.inputExchange();
            case SourceNode ignored -> ExchangeStrategy.FORWARD;
        };
    }

    private static dev.dhruv.streaming.api.Operator<?, ?> operatorOf(LogicalOperator node) {
        return switch (node) {
            case TransformNode transform -> transform.operator();
            case SinkNode sink -> sink.operator();
            case SourceNode ignored -> throw new IllegalStateException(
                    "a source is not run as an operator task: " + node.id());
        };
    }

    /**
     * Blocks until every task has stopped.
     *
     * @throws InterruptedException if the waiting thread is interrupted
     */
    @Override
    public void awaitTermination() throws InterruptedException {
        for (Thread thread : threads) {
            thread.join();
        }
    }

    /**
     * Stops the job.
     *
     * <p>Sources are cancelled first and operators after, so the pipeline drains forwards rather
     * than leaving downstream tasks blocked on gates nothing will fill. Tasks are then given a
     * grace period before being interrupted.
     */
    @Override
    public void close() {
        log.info("stopping job '{}'", graph.name());
        sourceTasks.forEach(SourceTask::cancel);
        operatorTasks.forEach(OperatorTask::cancel);

        for (Thread thread : threads) {
            try {
                thread.join(TimeUnit.SECONDS.toMillis(5));
                if (thread.isAlive()) {
                    log.warn("task {} did not stop within the grace period, interrupting",
                            thread.getName());
                    thread.interrupt();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                thread.interrupt();
                return;
            }
        }
        logMetrics();
    }

    /**
     * Returns each task's metrics, keyed by task id.
     *
     * @return per-task metric snapshots
     */
    public Map<String, Map<String, Long>> metrics() {
        Map<String, Map<String, Long>> byTask = new HashMap<>();
        sourceTasks.forEach(task -> byTask.put(task.taskId(), task.metrics().snapshot()));
        operatorTasks.forEach(task -> byTask.put(task.taskId(), task.metrics().snapshot()));
        return byTask;
    }

    private void logMetrics() {
        metrics().forEach((taskId, values) -> log.info("  {} -> {}", taskId, values));
    }

    private static String taskId(LogicalOperator node, int subtask) {
        return node.id() + "(" + (subtask + 1) + "/" + node.parallelism() + ")";
    }
}
