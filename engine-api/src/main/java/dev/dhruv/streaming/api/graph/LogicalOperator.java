package dev.dhruv.streaming.api.graph;

import java.util.List;

/**
 * One node in the logical job graph: what the user asked for, before the master decides how
 * many copies of it to run and where.
 *
 * <p>Sealed into exactly three kinds, because the engine treats them differently at points
 * that matter. Only a source has a barrier injected into it. Only a sink is told that a
 * checkpoint completed. Making the compiler enforce that a switch covers all three is worth
 * more than the flexibility of a single node type with nullable fields.
 *
 * @see SourceNode
 * @see TransformNode
 * @see SinkNode
 */
public sealed interface LogicalOperator
        permits SourceNode, TransformNode, SinkNode {

    /**
     * Returns this operator's stable identity.
     *
     * <p>Assigned explicitly by the user as a name, never derived from position in the graph.
     * That is a deliberate constraint rather than a convenience: state in a checkpoint is filed
     * under this id, so a job can gain an operator, lose one, or reorder two of them and still
     * restore the state belonging to the ones that stayed. An id derived from graph position
     * would silently reassign every piece of state the first time the graph changed shape.
     *
     * @return the operator id, unique within the job
     */
    String id();

    /**
     * Returns how many parallel subtasks this operator runs as.
     *
     * @return the parallelism, always at least one
     */
    int parallelism();

    /**
     * Returns the ids of the operators feeding this one. Empty for a source.
     *
     * @return upstream operator ids
     */
    List<String> upstreamIds();
}
