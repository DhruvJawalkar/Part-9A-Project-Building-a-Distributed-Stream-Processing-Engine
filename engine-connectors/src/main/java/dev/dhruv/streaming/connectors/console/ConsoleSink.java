package dev.dhruv.streaming.connectors.console;

import dev.dhruv.streaming.api.Collector;
import dev.dhruv.streaming.api.Operator;
import dev.dhruv.streaming.api.OperatorContext;
import dev.dhruv.streaming.api.StreamRecord;
import dev.dhruv.streaming.api.metrics.Counter;

/**
 * Prints every record it receives to standard output.
 *
 * <p>A sink is just an {@link Operator} whose output type is {@link Void} -- it receives
 * records and collects nothing. There is no separate sink interface in this engine, which keeps
 * the task run loop uniform: a sink task runs exactly the same loop as any other task and
 * simply has nowhere to send.
 *
 * <p>This one exists to make a running job observable with no infrastructure at all. It is also
 * a useful contrast with the Iceberg sink built in Phase 6: printing is inherently
 * at-least-once and unrecoverable -- a line already on the terminal cannot be taken back if the
 * job restarts from an earlier checkpoint. Exactly-once output is not a property of the engine
 * alone; it requires a sink that can hold writes back until a checkpoint completes.
 *
 * @param <T> the record type to print
 */
public final class ConsoleSink<T> implements Operator<T, Void> {

    private static final long serialVersionUID = 1L;

    private final String prefix;

    private transient Counter recordsWritten;

    /**
     * Creates a sink that prints records unprefixed.
     */
    public ConsoleSink() {
        this("");
    }

    /**
     * Creates a sink that prints each record behind a fixed prefix, so that two sinks in one
     * job can be told apart on the terminal.
     *
     * @param prefix text to print before each record
     */
    public ConsoleSink(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public void open(OperatorContext ctx) {
        this.recordsWritten = ctx.metrics().counter("records-written");
    }

    @Override
    public void processElement(StreamRecord<T> record, Collector<Void> out) {
        System.out.println(prefix + record.value());
        recordsWritten.increment();
    }
}
