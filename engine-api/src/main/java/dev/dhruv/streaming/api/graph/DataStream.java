package dev.dhruv.streaming.api.graph;

import dev.dhruv.streaming.api.ExchangeStrategy;
import dev.dhruv.streaming.api.FilterFunction;
import dev.dhruv.streaming.api.FilterOperator;
import dev.dhruv.streaming.api.KeySelector;
import dev.dhruv.streaming.api.Operator;
import dev.dhruv.streaming.api.TimestampAssigner;

import java.time.Duration;
import java.util.Objects;

/**
 * A handle to the records produced by one operator, and the thing you call the next operator
 * on.
 *
 * <p>A handle, not a collection. Nothing is flowing while a job is being described; calling
 * {@link #filter} does not filter anything, it records that a filter belongs here. The value
 * of holding one in a variable is that it can be used more than once: two operators reading the
 * same handle fan out from one upstream, and two handles are what an interval join needs to
 * name its sides.
 *
 * @param <T> the type of record on this stream
 */
public final class DataStream<T> {

    private final JobGraph.Builder builder;
    private final GraphNode node;

    DataStream(JobGraph.Builder builder, GraphNode node) {
        this.builder = builder;
        this.node = node;
    }

    /**
     * Sets how many parallel subtasks the operator producing this stream runs as.
     *
     * <p>Applies to the operator just declared, which is why it reads as a suffix:
     * {@code .filter("drop-bots", new BotFilter()).parallelism(4)} means four filters, not four
     * of whatever comes next.
     *
     * @param parallelism number of subtasks, at least one
     * @return this stream
     */
    public DataStream<T> parallelism(int parallelism) {
        node.parallelism = parallelism;
        return this;
    }

    /**
     * Declares how to read event time out of this stream's records, and how far out of order
     * they may arrive.
     *
     * <p>Valid on a source only. Event time enters a job exactly once, at the point records do,
     * and is propagated by the engine from there on -- reassigning it midstream would mean two
     * operators disagreeing about what time it is.
     *
     * <p>The out-of-orderness value is a straight trade of latency against completeness. It is
     * how long the engine waits, in event time, for stragglers before declaring a point in time
     * settled. Set it to zero and every window closes the instant its last in-order event
     * arrives, dropping anything delayed; set it generously and every window closes that much
     * later.
     *
     * @param assigner       how to read a record's event time
     * @param outOfOrderness how far behind the maximum seen event time to hold the watermark
     * @return this stream
     * @throws InvalidJobGraphException if this is not a source
     */
    public DataStream<T> withEventTime(TimestampAssigner<T> assigner, Duration outOfOrderness) {
        requireSource("withEventTime");
        node.timestampAssigner = Objects.requireNonNull(assigner, "assigner");
        node.outOfOrderness = Objects.requireNonNull(outOfOrderness, "outOfOrderness");
        return this;
    }

    /**
     * Declares how long a subtask of this source may produce nothing before it is treated as
     * idle and excluded from the downstream watermark minimum.
     *
     * <p>Valid on a source only. Without idleness, a downstream operator takes the minimum
     * watermark across all of its inputs, so a single partition that has gone quiet pins the
     * event-time clock of the whole job at whatever it last reported, and every window
     * everywhere stops firing while the job otherwise looks perfectly healthy.
     *
     * <p>The cost is that a channel declared idle no longer holds the clock back, so a record
     * arriving on it later may well be late. Idleness trades a little correctness for liveness,
     * deliberately and with the dial exposed.
     *
     * @param idleTimeout how long silence is tolerated before the channel is marked idle;
     *                    {@link Duration#ZERO} disables idleness detection entirely
     * @return this stream
     * @throws InvalidJobGraphException if this is not a source
     */
    public DataStream<T> withIdleness(Duration idleTimeout) {
        requireSource("withIdleness");
        node.idleTimeout = Objects.requireNonNull(idleTimeout, "idleTimeout");
        return this;
    }

    /**
     * Adds an operator that drops records failing a predicate.
     *
     * @param id       stable operator id, unique within the job
     * @param function the predicate
     * @return a handle to the surviving records
     */
    public DataStream<T> filter(String id, FilterFunction<T> function) {
        return process(id, new FilterOperator<>(function));
    }

    /**
     * Adds an operator to this stream.
     *
     * <p>The general case, of which filtering is one shape. The new operator's input exchange
     * is {@link ExchangeStrategy#FORWARD}, meaning subtask {@code i} feeds subtask {@code i} --
     * which is what makes it eligible to be chained into the same thread as its upstream when
     * the two have equal parallelism.
     *
     * @param id       stable operator id, unique within the job
     * @param operator the user logic
     * @param <R>      the type it produces
     * @return a handle to the records it produces
     */
    public <R> DataStream<R> process(String id, Operator<T, R> operator) {
        GraphNode next = builder.addNode(id, GraphNode.Kind.TRANSFORM);
        next.operator = Objects.requireNonNull(operator, "operator");
        next.parallelism = node.parallelism;
        next.upstreamIds.add(node.id);
        next.inputExchange = ExchangeStrategy.FORWARD;
        return new DataStream<>(builder, next);
    }

    /**
     * Repartitions this stream by key, so that every record sharing a key reaches the same
     * downstream subtask.
     *
     * <p>Creates no operator and no task of its own. Keying is a property of the <em>edge</em>
     * into whatever comes next, not a step that does work, so what this returns is a stream that
     * remembers it must be hashed. The name is carried through to logs and metrics so the
     * exchange can be identified by what the author called it.
     *
     * @param name     what to call this keying, for logs and metrics
     * @param selector how to extract the key
     * @param <K>      the key type
     * @return a keyed handle, on which keyed operators may be placed
     */
    public <K> KeyedStream<K, T> keyBy(String name, KeySelector<T, K> selector) {
        return new KeyedStream<>(builder, node,
                Objects.requireNonNull(selector, "selector"),
                Objects.requireNonNull(name, "name"));
    }

    /**
     * Terminates this stream in a sink.
     *
     * @param id   stable operator id, unique within the job
     * @param sink the sink logic, an operator that produces nothing
     * @return a handle on which the sink's parallelism may be set
     */
    public DataStreamSink sink(String id, Operator<T, Void> sink) {
        GraphNode next = builder.addNode(id, GraphNode.Kind.SINK);
        next.operator = Objects.requireNonNull(sink, "sink");
        next.parallelism = node.parallelism;
        next.upstreamIds.add(node.id);
        next.inputExchange = ExchangeStrategy.FORWARD;
        return new DataStreamSink(next);
    }

    /**
     * Returns the id of the operator producing this stream. Useful in tests and in error
     * messages; the graph is addressed by these names everywhere else.
     *
     * @return the producing operator's id
     */
    public String id() {
        return node.id;
    }

    GraphNode node() {
        return node;
    }

    private void requireSource(String method) {
        if (node.kind != GraphNode.Kind.SOURCE) {
            throw new InvalidJobGraphException(
                    method + " is only valid on a source, but '" + node.id + "' is a "
                            + node.kind.name().toLowerCase() + "; event time enters a job at"
                            + " its sources and is propagated from there");
        }
    }
}
