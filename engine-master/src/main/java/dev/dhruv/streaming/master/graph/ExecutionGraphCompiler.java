package dev.dhruv.streaming.master.graph;

import dev.dhruv.streaming.metadata.RegisteredWorker;
import dev.dhruv.streaming.api.graph.JobGraph;
import dev.dhruv.streaming.api.graph.LogicalOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Turns a logical job graph into a physical plan against the workers currently registered.
 *
 * <p>Three steps, in order: expand each operator into its parallel instances, fuse adjacent
 * operators that can share a thread, and spread the resulting slices across the workers.
 *
 * <h2>What a slot is here</h2>
 *
 * <p>The unit of scheduling is one <em>vertical slice</em> of a chain group: subtask {@code i}
 * of every operator in the chain, together. So a chain of {@code source -> filter} at
 * parallelism four produces four slices, not one, and they are distributed across workers
 * independently.
 *
 * <p>This mirrors Flink rather than Storm. The alternative -- assigning a whole chain group to
 * a single worker -- would keep the scheduler simpler, but a job of two chain groups could then
 * only ever occupy two workers however parallel its operators were, and adding workers would
 * stop helping. Slicing vertically is what makes parallelism and cluster size independent.
 *
 * <h2>What this scheduler does not do</h2>
 *
 * <p>Round-robin and nothing else. It does not consider how loaded a worker is, how large the
 * state of an operator is likely to be, whether two communicating slices would be better
 * co-located, or whether a worker has the memory for what it is being handed. Every one of
 * those is a real scheduler's job and every one of them would obscure the mechanism being shown
 * here, which is simply that the master decides and the workers are told.
 */
public final class ExecutionGraphCompiler {

    private static final Logger log = LoggerFactory.getLogger(ExecutionGraphCompiler.class);

    private ExecutionGraphCompiler() {
    }

    /**
     * Compiles a logical graph against a set of workers.
     *
     * @param logical the validated job graph
     * @param workers the workers currently registered, in a stable order
     * @return the physical plan
     * @throws IllegalStateException if no workers are registered, or if the job needs more
     *                               slots than the cluster has
     */
    public static ExecutionGraph compile(JobGraph logical, List<RegisteredWorker> workers) {
        if (workers.isEmpty()) {
            throw new IllegalStateException(
                    "cannot schedule job '" + logical.name() + "': no workers registered");
        }

        List<ExecutionVertex> vertices = expand(logical);
        List<ChainGroup> chains = ChainBuilder.build(logical);
        Map<String, TaskAssignment> assignments = assign(chains, workers);

        log.info("compiled job '{}': {} operators -> {} vertices -> {} chain groups -> {} tasks"
                        + " across {} workers",
                logical.name(), logical.operators().size(), vertices.size(), chains.size(),
                assignments.size(), workers.size());
        for (ChainGroup chain : chains) {
            log.info("  chain '{}' x{} : {}", chain.id(), chain.parallelism(),
                    String.join(" -> ", chain.operatorIds()));
        }

        return new ExecutionGraph(logical.jobId(), vertices, chains, assignments);
    }

    /**
     * Step one: one operator running four ways becomes four things that need somewhere to run.
     */
    private static List<ExecutionVertex> expand(JobGraph logical) {
        List<ExecutionVertex> vertices = new ArrayList<>();
        for (LogicalOperator operator : logical.operatorsInTopologicalOrder()) {
            for (int subtask = 0; subtask < operator.parallelism(); subtask++) {
                vertices.add(new ExecutionVertex(
                        operator.id(),
                        subtask,
                        operator.parallelism(),
                        ChainBuilder.inputExchangeOf(operator),
                        operator.upstreamIds()));
            }
        }
        return vertices;
    }

    /**
     * Step three: spread the vertical slices across workers, round-robin.
     *
     * <p>The counter runs across chain groups rather than restarting for each one. Restarting it
     * would put subtask zero of every chain on the first worker, which is the arrangement most
     * likely to make one machine the bottleneck for the entire job.
     */
    private static Map<String, TaskAssignment> assign(
            List<ChainGroup> chains, List<RegisteredWorker> workers) {

        int totalSlots = workers.stream().mapToInt(RegisteredWorker::slots).sum();
        int requiredSlots = chains.stream().mapToInt(ChainGroup::parallelism).sum();
        if (requiredSlots > totalSlots) {
            throw new IllegalStateException(
                    "job needs " + requiredSlots + " slots but the cluster offers " + totalSlots
                            + " across " + workers.size() + " workers; reduce parallelism or"
                            + " start more workers");
        }

        Map<String, TaskAssignment> assignments = new LinkedHashMap<>();
        int next = 0;
        for (ChainGroup chain : chains) {
            for (int subtask = 0; subtask < chain.parallelism(); subtask++) {
                RegisteredWorker worker = workers.get(next++ % workers.size());
                TaskAssignment assignment =
                        new TaskAssignment(chain.id(), subtask, worker.workerId());
                assignments.put(assignment.key(), assignment);
            }
        }
        return assignments;
    }
}
