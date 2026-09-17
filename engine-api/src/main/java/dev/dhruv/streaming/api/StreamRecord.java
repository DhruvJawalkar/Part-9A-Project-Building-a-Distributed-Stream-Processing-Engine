package dev.dhruv.streaming.api;

/**
 * A user data record together with its event-time timestamp.
 *
 * <p>The timestamp is the time the event <em>happened</em>, assigned upstream at the client,
 * not the time it arrived here. Every windowing and join decision in the engine is made
 * against this field, which is what makes a replay of the same input produce the same output
 * no matter how fast or slow it is replayed.
 *
 * <h2>What the engine asks of {@code T}</h2>
 *
 * <p>The value must implement {@link java.io.Serializable}. Any edge that is not a forward
 * exchange between co-located subtasks means the record leaves its JVM -- serialized, sent over
 * gRPC, and rebuilt on another worker -- and this engine uses Java serialization to do it.
 *
 * <p>Java records are <em>not</em> serializable unless they declare it, which is an easy thing
 * to miss because a job runs perfectly well in a single process without it. The failure surfaces
 * the first time a record crosses a process boundary, which is the first time the job is run on
 * a real cluster.
 *
 * @param value     the user's record, which must be serializable
 * @param timestamp event time in milliseconds since the epoch
 * @param <T>       the user record type
 */
public record StreamRecord<T>(T value, long timestamp) implements StreamElement {

    /**
     * Returns a copy of this record carrying a different value but the same event time.
     * Used by operators that transform a record without moving it in time.
     *
     * @param newValue the replacement value
     * @param <R>      the new record type
     * @return a record with {@code newValue} at this record's timestamp
     */
    public <R> StreamRecord<R> withValue(R newValue) {
        return new StreamRecord<>(newValue, timestamp);
    }
}
