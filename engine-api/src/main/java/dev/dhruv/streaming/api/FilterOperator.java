package dev.dhruv.streaming.api;

import java.util.Objects;

/**
 * Adapts a {@link FilterFunction} to the {@link Operator} interface.
 *
 * <p>Deliberately a named class rather than a lambda inside the graph builder. A reader who
 * wonders what {@code .filter(...)} actually becomes can open this file and see the entire
 * answer in four lines: a filter is an operator that sometimes declines to collect.
 *
 * @param <T> the record type
 */
public final class FilterOperator<T> implements Operator<T, T> {

    private final FilterFunction<T> function;

    /**
     * Wraps a filter function.
     *
     * @param function the predicate to apply
     */
    public FilterOperator(FilterFunction<T> function) {
        this.function = Objects.requireNonNull(function, "function");
    }

    @Override
    public void processElement(StreamRecord<T> record, Collector<T> out) throws Exception {
        if (function.keep(record.value())) {
            out.collect(record.value());
        }
    }
}
