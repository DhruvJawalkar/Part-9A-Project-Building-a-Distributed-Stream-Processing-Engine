package dev.dhruv.streaming.api;

import java.io.Serializable;

/**
 * Creates one output from a matching left/right pair in an {@link IntervalJoinOperator}.
 *
 * <p>The function is serializable because it is part of the job graph. It is invoked once for
 * every matching pair, at the arrival of the pair's second record.
 *
 * @param <L> left input type
 * @param <R> right input type
 * @param <O> output type
 */
@FunctionalInterface
public interface JoinFunction<L, R, O> extends Serializable {

    /**
     * Joins a matching pair.
     *
     * @param left the left record
     * @param right the right record
     * @return the output to emit
     * @throws Exception if joining fails; the task fails and its normal recovery applies
     */
    O join(L left, R right) throws Exception;
}
