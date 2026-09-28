package dev.dhruv.streaming.api;

import java.io.Serializable;
import java.util.Objects;
import java.util.function.Function;

/**
 * One value drawn from either the left or right input of a two-input operator.
 *
 * <p>An interval join has one physical input in this teaching engine, so callers tag records
 * from its two logical inputs with this type. The tag travels with the record and is retained
 * in state when the record must wait for its partner.
 *
 * @param <L> type carried by the left alternative
 * @param <R> type carried by the right alternative
 */
public sealed interface Either<L, R> extends Serializable permits Either.Left, Either.Right {

    /** Returns whether this value carries a left record. */
    boolean isLeft();

    /** Returns the left record, or throws when this is a right value. */
    L left();

    /** Returns the right record, or throws when this is a left value. */
    R right();

    /**
     * Applies the appropriate mapper and returns its result.
     *
     * <p>This is the most convenient way to use an {@code Either} where both alternatives
     * must become one type, such as extracting the shared key for the two inputs of a join.
     *
     * @param onLeft mapper for a left value
     * @param onRight mapper for a right value
     * @param <T> common result type
     * @return the selected mapper's result
     */
    <T> T fold(Function<? super L, ? extends T> onLeft,
               Function<? super R, ? extends T> onRight);

    /** Wraps a left record. */
    static <L, R> Either<L, R> left(L value) {
        return new Left<>(value);
    }

    /** Wraps a right record. */
    static <L, R> Either<L, R> right(R value) {
        return new Right<>(value);
    }

    /** The left alternative. */
    record Left<L, R>(L value) implements Either<L, R> {
        public Left {
            Objects.requireNonNull(value, "value");
        }

        @Override
        public boolean isLeft() {
            return true;
        }

        @Override
        public L left() {
            return value;
        }

        @Override
        public R right() {
            throw new IllegalStateException("this Either contains a left value");
        }

        @Override
        public <T> T fold(Function<? super L, ? extends T> onLeft,
                          Function<? super R, ? extends T> onRight) {
            return Objects.requireNonNull(onLeft, "onLeft").apply(value);
        }
    }

    /** The right alternative. */
    record Right<L, R>(R value) implements Either<L, R> {
        public Right {
            Objects.requireNonNull(value, "value");
        }

        @Override
        public boolean isLeft() {
            return false;
        }

        @Override
        public L left() {
            throw new IllegalStateException("this Either contains a right value");
        }

        @Override
        public R right() {
            return value;
        }

        @Override
        public <T> T fold(Function<? super L, ? extends T> onLeft,
                          Function<? super R, ? extends T> onRight) {
            return Objects.requireNonNull(onRight, "onRight").apply(value);
        }
    }
}
