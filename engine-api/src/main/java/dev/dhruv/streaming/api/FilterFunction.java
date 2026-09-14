package dev.dhruv.streaming.api;

import java.io.Serializable;

/**
 * Decides whether a record continues downstream.
 *
 * <p>A convenience over writing an {@link Operator} that conditionally collects its input.
 * The job graph wraps one of these in a {@link FilterOperator} at build time, so nothing
 * downstream of the builder needs to know filters are special.
 *
 * @param <T> the record type
 */
@FunctionalInterface
public interface FilterFunction<T> extends Serializable {

    /**
     * Returns whether to keep the record.
     *
     * @param value the record
     * @return true to pass it downstream, false to drop it
     * @throws Exception any failure; the runtime fails the task
     */
    boolean keep(T value) throws Exception;
}
