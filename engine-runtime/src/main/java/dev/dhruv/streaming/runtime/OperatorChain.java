package dev.dhruv.streaming.runtime;

import dev.dhruv.streaming.api.Collector;
import dev.dhruv.streaming.api.Operator;
import dev.dhruv.streaming.api.OperatorContext;
import dev.dhruv.streaming.api.StreamRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * Several operators fused into one, so that a whole run of them shares a thread.
 *
 * <h2>What chaining actually removes</h2>
 *
 * <p>Two operators on either side of an ordinary edge exchange a record by serializing it,
 * batching it, handing it to another thread or another machine, and deserializing it on the far
 * side. Two operators in a chain exchange a record by calling a method.
 *
 * <p>The mechanism is a chain of collectors, built once in {@link #open}. Each operator is
 * handed a collector that calls the next operator's {@code processElement} directly; the last
 * one gets the task's real output. So {@code out.collect(value)} inside a filter is, at runtime,
 * a call into the map behind it -- the same stack, the same thread, no copy of the record at
 * all.
 *
 * <p>That last point is worth dwelling on, because it is the one place in the engine where an
 * operator could interfere with another. A record passed along a chain is the <em>same object</em>
 * that the upstream operator emitted. An operator that emits a mutable object and then keeps
 * modifying it is modifying something its downstream neighbour is already looking at. Real
 * engines either copy defensively or document the constraint loudly; this one documents it,
 * because copying every record would undo most of what chaining bought.
 */
public final class OperatorChain implements Operator<Object, Object> {

    private static final long serialVersionUID = 1L;

    private final List<Operator<Object, Object>> operators;

    private transient List<Collector<Object>> collectors;

    /**
     * Event time of the record currently moving through the chain.
     *
     * <p>A field rather than a parameter because {@link Collector#collect(Object)} deliberately
     * does not take a timestamp -- an operator transforming a record should not have to restate
     * when the underlying event happened. Safe as a field because a chain runs on exactly one
     * thread, which is the guarantee the whole engine is built on.
     */
    private transient long currentTimestamp;

    /**
     * Creates a chain.
     *
     * @param operators the fused operators, in the order records flow through them
     */
    @SuppressWarnings("unchecked")
    public OperatorChain(List<? extends Operator<?, ?>> operators) {
        if (operators.isEmpty()) {
            throw new IllegalArgumentException("a chain needs at least one operator");
        }
        this.operators = List.copyOf((List<Operator<Object, Object>>) operators);
    }

    @Override
    public void open(OperatorContext ctx) throws Exception {
        for (Operator<Object, Object> operator : operators) {
            operator.open(ctx);
        }
    }

    @Override
    public void processElement(StreamRecord<Object> record, Collector<Object> out)
            throws Exception {
        // Seed the event time of the record entering the chain. Without this, the first
        // operator's out.collect(value) -- the overload that deliberately takes no timestamp --
        // would stamp its output with zero, and every operator behind it would believe the event
        // happened at the epoch. Nothing would fail; windows downstream would simply be wrong.
        currentTimestamp = record.timestamp();
        operators.getFirst().processElement(record, collectorFor(0, out));
    }

    @Override
    public void onWatermark(long watermark, Collector<Object> out) throws Exception {
        // Every operator in the chain sees the clock advance, in flow order, so that one which
        // emits on a watermark does so into the operator behind it rather than past it.
        for (int i = 0; i < operators.size(); i++) {
            operators.get(i).onWatermark(watermark, collectorFor(i, out));
        }
    }

    @Override
    public void close() throws Exception {
        // Closed in reverse, so an operator is still able to emit into its downstream neighbour
        // while shutting down -- which a buffering operator flushing its last records needs.
        Exception failure = null;
        for (int i = operators.size() - 1; i >= 0; i--) {
            try {
                operators.get(i).close();
            } catch (Exception e) {
                failure = e;
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * Returns the collector for the operator at {@code index}: either a call into the next
     * operator, or the task's real output if this is the last one.
     */
    private Collector<Object> collectorFor(int index, Collector<Object> out) {
        if (collectors == null) {
            buildCollectors(out);
        }
        return collectors.get(index);
    }

    private void buildCollectors(Collector<Object> out) {
        List<Collector<Object>> built = new ArrayList<>(operators.size());
        for (int i = 0; i < operators.size(); i++) {
            built.add(i == operators.size() - 1
                    ? out
                    : new ChainedCollector(operators.get(i + 1), i + 1, out));
        }
        this.collectors = built;
    }

    /**
     * A collector that is really a call into the next operator.
     */
    private final class ChainedCollector implements Collector<Object> {

        private final Operator<Object, Object> next;
        private final int nextIndex;
        private final Collector<Object> tailOutput;

        ChainedCollector(Operator<Object, Object> next, int nextIndex,
                         Collector<Object> tailOutput) {
            this.next = next;
            this.nextIndex = nextIndex;
            this.tailOutput = tailOutput;
        }

        @Override
        public void collect(Object value) {
            // No timestamp given, so the record keeps the event time it arrived with. The task's
            // OutputCollector holds that; within a chain it is simply passed along.
            collect(value, currentTimestamp);
        }

        @Override
        public void collect(Object value, long timestamp) {
            try {
                long previous = currentTimestamp;
                currentTimestamp = timestamp;
                try {
                    next.processElement(new StreamRecord<>(value, timestamp),
                            collectorFor(nextIndex, tailOutput));
                } finally {
                    currentTimestamp = previous;
                }
            } catch (Exception e) {
                throw new ChainedOperatorException(
                        "operator at position " + nextIndex + " in the chain failed", e);
            }
        }
    }

    /**
     * Wraps a failure from inside a chain so that it unwinds through the calling operators
     * rather than being caught by one of them.
     */
    public static final class ChainedOperatorException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        ChainedOperatorException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
