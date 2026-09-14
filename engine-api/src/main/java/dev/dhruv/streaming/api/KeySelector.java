package dev.dhruv.streaming.api;

import java.io.Serializable;

/**
 * Extracts the partitioning key from a record.
 *
 * <p>Must be deterministic and pure. The engine calls it on the sending side to route the
 * record and again on the receiving side to scope state, in different processes and possibly
 * after a restart. If two calls disagree, a record lands at a subtask that does not hold its
 * key's state, and the resulting corruption is silent.
 *
 * <p>The extracted key's {@code hashCode} must also be stable across JVMs, which rules out
 * anything inheriting the identity hash. Strings, boxed primitives and records of those are
 * safe; an enum is not, and an object without a {@code hashCode} override is not.
 *
 * @param <T> the record type
 * @param <K> the key type
 */
@FunctionalInterface
public interface KeySelector<T, K> extends Serializable {

    /**
     * Returns the key for a record.
     *
     * @param value the record
     * @return its key, never null
     * @throws Exception any failure; the runtime fails the task
     */
    K getKey(T value) throws Exception;
}
