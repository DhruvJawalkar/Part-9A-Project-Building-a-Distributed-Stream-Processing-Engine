package dev.dhruv.streaming.runtime;

import dev.dhruv.streaming.api.ExchangeStrategy;
import dev.dhruv.streaming.api.JobExecutor;
import dev.dhruv.streaming.api.StreamElement;
import dev.dhruv.streaming.api.graph.JobGraph;
import dev.dhruv.streaming.api.graph.LogicalOperator;
import dev.dhruv.streaming.api.graph.SinkNode;
import dev.dhruv.streaming.api.graph.SourceNode;
import dev.dhruv.streaming.api.graph.TransformNode;
import dev.dhruv.streaming.runtime.metrics.TaskMetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Runs a whole job inside one JVM: one thread per subtask, blocking queues between them.
 *
 * <p>Phase 1's engine, entire. There is no master, no scheduler and no network -- the graph is
 * walked in topological order, a queue is created for every subtask, and each task is handed
 * the queues belonging to whatever comes next.
 *
 * <p>It is worth being clear about what this is and is not. It is a genuine implementation of
 * task execution, backpressure and record routing, and the {@link OperatorTask} it starts is
 * the same one a worker will run in Phase 2. It is not distribution: every task is in this
 * process, so nothing is serialized, nothing can partially fail, and no task can outlive
 * another. Phase 2 replaces the wiring here with a scheduler and gRPC streams while leaving the
 * tasks themselves alone, which is the test of whether the boundary was drawn in the right
 * place.
 */
public final class LocalJobExecutor implements JobExecutor {

    private static final Logger log = LoggerFactory.getLogger(LocalJobExecutor.class);

    /**
     * Capacity of each subtask's inbound queue.
     *
     * <p>The number that makes backpressure visible. Large enough that a brief hiccup does not
     * stall the pipeline, small enough that a genuinely slow operator makes its upstream wait
     * rather than accumulating an unbounded backlog in memory.
     */
    private static final int QUEUE_CAPACITY = 1_024;

    private final JobGraph graph;
    private final List<Thread> threads = new ArrayList<>();
    private final List<SourceTask> sourceTasks = new ArrayList<>();
    private final List<OperatorTask> operatorTasks = new ArrayList<>();

    /** Inbound queues, one list of subtask queues per operator id. */
    private final Map<String, List<BlockingQueue<StreamElement>>> inboundQueues = new HashMap<>();

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
     * <p>Tasks are created in topological order so that a task's downstream queues already
     * exist by the time it needs to be handed them.
     */
    @Override
    public void start() {
        log.info("starting job '{}' ({}) with {} operators",
                graph.name(), graph.jobId(), graph.operators().size());

        for (LogicalOperator operator : graph.operatorsInTopologicalOrder()) {
            inboundQueues.put(operator.id(), createQueues(operator.parallelism()));
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

    private void startSourceSubtasks(SourceNode source) {
        for (int subtask = 0; subtask < source.parallelism(); subtask++) {
            String taskId = taskId(source, subtask);
            TaskMetricGroup metrics = new TaskMetricGroup(source.id(), subtask);
            SourceTask task = new SourceTask(
                    taskId,
                    // A private copy per subtask. Sharing one instance across four threads
                    // is the bug this prevents; see TaskInstances.
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
                            + " Phase 3 along with keyed state. Phase 1 runs forward and"
                            + " rebalance edges only.");
        }

        List<BlockingQueue<StreamElement>> queues = inboundQueues.get(node.id());
        for (int subtask = 0; subtask < node.parallelism(); subtask++) {
            String taskId = taskId(node, subtask);
            TaskMetricGroup metrics = new TaskMetricGroup(node.id(), subtask);
            OperatorTask task = new OperatorTask(
                    taskId,
                    TaskInstances.copyOf(operatorOf(node)),
                    queues.get(subtask),
                    outputFor(node, subtask),
                    upstreamSubtasksFeeding(node, inputExchange),
                    metrics);

            operatorTasks.add(task);
            threads.add(new Thread(task, taskId));
        }
    }

    /**
     * Returns how many upstream subtasks send to each subtask of this node.
     *
     * <p>Under a forward exchange the answer is one, because subtask {@code i} hears only from
     * subtask {@code i}. Under any redistributing exchange every upstream subtask may send to
     * every downstream one, so the answer is all of them. The task uses this to know when it has
     * heard end-of-stream from everything that feeds it.
     */
    private int upstreamSubtasksFeeding(LogicalOperator node, ExchangeStrategy inputExchange) {
        if (inputExchange == ExchangeStrategy.FORWARD) {
            return 1;
        }
        return node.upstreamIds().stream()
                .map(id -> graph.operator(id).orElseThrow())
                .mapToInt(LogicalOperator::parallelism)
                .sum();
    }

    /**
     * Builds the output for one subtask: the inbound queues of everything downstream of it.
     *
     * <p>A node feeding two downstream operators gets an output per downstream, because the two
     * may route differently. Phase 1's jobs are linear, so in practice this is zero or one.
     */
    private Output outputFor(LogicalOperator node, int subtaskIndex) {
        List<String> downstreamIds = graph.downstreamIdsOf(node.id());
        if (downstreamIds.isEmpty()) {
            return new LocalOutput(List.of(), ExchangeStrategy.FORWARD, subtaskIndex);
        }
        if (downstreamIds.size() > 1) {
            throw new UnsupportedOperationException(
                    "operator '" + node.id() + "' fans out to " + downstreamIds
                            + "; Phase 1 runs linear pipelines only");
        }

        String downstreamId = downstreamIds.getFirst();
        LogicalOperator downstream = graph.operator(downstreamId).orElseThrow();
        return new LocalOutput(
                inboundQueues.get(downstreamId), exchangeInto(downstream), subtaskIndex);
    }

    private static ExchangeStrategy exchangeInto(LogicalOperator node) {
        return switch (node) {
            case TransformNode transform -> transform.inputExchange();
            case SinkNode sink -> sink.inputExchange();
            case SourceNode ignored -> throw new IllegalStateException(
                    "a source has no input edge: " + node.id());
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
     * <p>Sources are cancelled first and operators after, so the pipeline drains forwards
     * rather than leaving downstream tasks blocked on queues nothing will drain. Tasks are then
     * given a grace period before being interrupted.
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
     * Returns each task's metrics, keyed by task id. Phase 7 exposes these over HTTP; for now
     * they are what tests assert on and what is logged at shutdown.
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

    private static List<BlockingQueue<StreamElement>> createQueues(int parallelism) {
        List<BlockingQueue<StreamElement>> queues = new ArrayList<>(parallelism);
        for (int i = 0; i < parallelism; i++) {
            queues.add(new ArrayBlockingQueue<>(QUEUE_CAPACITY));
        }
        return queues;
    }

    private static String taskId(LogicalOperator node, int subtask) {
        return node.id() + "(" + (subtask + 1) + "/" + node.parallelism() + ")";
    }
}
