package dev.dhruv.streaming.master.graph;

/**
 * A decision: this vertical slice of the pipeline runs on that worker.
 *
 * @param chainGroupId which chain group
 * @param subtaskIndex which slice of it
 * @param workerId     where it runs
 */
public record TaskAssignment(String chainGroupId, int subtaskIndex, String workerId) {

    /**
     * Returns the key this assignment is stored under, in etcd and in the execution graph.
     *
     * @return {@code chainGroupId:subtaskIndex}
     */
    public String key() {
        return chainGroupId + ":" + subtaskIndex;
    }
}
