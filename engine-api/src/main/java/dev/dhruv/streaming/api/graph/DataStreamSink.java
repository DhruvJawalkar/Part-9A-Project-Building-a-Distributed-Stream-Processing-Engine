package dev.dhruv.streaming.api.graph;

/**
 * A handle to a declared sink.
 *
 * <p>Deliberately almost empty. A sink is the end of a stream, so returning a
 * {@link DataStream} from it would invite code that reads as though output continues past the
 * point where it stops. The one thing still worth saying about a sink after declaring it is how
 * parallel it should be.
 */
public final class DataStreamSink {

    private final GraphNode node;

    DataStreamSink(GraphNode node) {
        this.node = node;
    }

    /**
     * Sets how many parallel subtasks this sink runs as.
     *
     * <p>Often lower than the operators feeding it. A sink usually writes somewhere that does
     * not benefit from concurrency the way computation does, and in the case of a file-based
     * sink, more subtasks means proportionally more small files per commit.
     *
     * @param parallelism number of subtasks, at least one
     * @return this handle
     */
    public DataStreamSink parallelism(int parallelism) {
        node.parallelism = parallelism;
        return this;
    }

    /**
     * Returns this sink's operator id.
     *
     * @return the sink's id
     */
    public String id() {
        return node.id;
    }
}
