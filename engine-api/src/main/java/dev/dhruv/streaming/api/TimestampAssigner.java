package dev.dhruv.streaming.api;

import java.io.Serializable;

/**
 * Pulls the event time out of a record.
 *
 * <p>Event time is a field the producer wrote, not something the engine can work out. This
 * interface is where the engine is told which field that is. Returning "now" from here is the
 * mistake that quietly turns an event-time job into a processing-time job: it will still run,
 * still produce output, and stop being reproducible.
 *
 * @param <T> the record type
 */
@FunctionalInterface
public interface TimestampAssigner<T> extends Serializable {

    /**
     * Returns the record's event time, in milliseconds since the epoch.
     *
     * @param value the record
     * @return when the event happened
     */
    long extractTimestamp(T value);
}
