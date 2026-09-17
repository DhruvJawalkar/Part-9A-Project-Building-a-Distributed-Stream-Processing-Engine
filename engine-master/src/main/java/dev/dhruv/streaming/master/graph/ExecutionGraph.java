package dev.dhruv.streaming.master.graph;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The physical plan: what actually runs, and where.
 *
 * <p>Kept as a separate type from {@link dev.dhruv.streaming.api.graph.JobGraph} rather than
 * produced by mutating it. The logical graph is what the user meant and never changes; this is
 * a decision about how to realise it against the workers that happened to be registered at
 * submission time. Compile the same job twice against different workers and you get two of
 * these and one of those, which is the distinction made concrete.
 *
 * @param jobId       the job this plan belongs to
 * @param vertices    every parallel instance of every operator
 * @param chainGroups the fused runs of operators, each scheduled as a unit
 * @param assignments where each vertical slice runs, keyed by {@code chainGroupId:subtaskIndex}
 */
public record ExecutionGraph(
        String jobId,
        List<ExecutionVertex> vertices,
        List<ChainGroup> chainGroups,
        Map<String, TaskAssignment> assignments
) {

    /**
     * Compact constructor, defensively copying.
     */
    public ExecutionGraph {
        vertices = List.copyOf(vertices);
        chainGroups = List.copyOf(chainGroups);
        assignments = Map.copyOf(assignments);
    }

    /**
     * Looks up a chain group by id.
     *
     * @param chainGroupId the group id
     * @return the group, if this plan has one
     */
    public Optional<ChainGroup> chainGroup(String chainGroupId) {
        return chainGroups.stream().filter(group -> group.id().equals(chainGroupId)).findFirst();
    }

    /**
     * Returns the assignments handed to one worker.
     *
     * @param workerId the worker
     * @return its slices, in assignment order
     */
    public List<TaskAssignment> assignmentsFor(String workerId) {
        return assignments.values().stream()
                .filter(assignment -> assignment.workerId().equals(workerId))
                .sorted(java.util.Comparator.comparing(TaskAssignment::key))
                .toList();
    }

    /**
     * Returns the distinct workers this plan depends on.
     *
     * @return worker ids
     */
    public List<String> workerIds() {
        return assignments.values().stream()
                .map(TaskAssignment::workerId).distinct().sorted().toList();
    }

    /**
     * Returns how many tasks this plan starts in total: one per vertical slice of each chain.
     *
     * @return the task count
     */
    public int taskCount() {
        return assignments.size();
    }
}
