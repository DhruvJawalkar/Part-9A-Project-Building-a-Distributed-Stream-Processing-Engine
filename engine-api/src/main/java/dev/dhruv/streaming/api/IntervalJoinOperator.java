package dev.dhruv.streaming.api;

import dev.dhruv.streaming.api.state.ListState;
import dev.dhruv.streaming.api.state.ValueState;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A keyed, event-time interval join over records tagged as {@link Either left} or right.
 *
 * <p>A pair matches when {@code lowerBound <= rightTimestamp - leftTimestamp <= upperBound};
 * both endpoints are inclusive. Each arriving record is compared with the records already
 * buffered on the opposite side, then retained itself. Consequently every pair is emitted
 * exactly once: specifically when its second record arrives, independent of which input was
 * first.
 *
 * <p>Bounds may be negative, which is useful when the right event is expected to precede the
 * left one. Both bounds are expressed in event-time milliseconds and {@code lowerBound} must
 * not exceed {@code upperBound}.
 *
 * <h2>Watermark cleanup</h2>
 *
 * <p>The operator uses two keyed {@link ListState lists}: {@code interval-join-left} and
 * {@code interval-join-right}. A left record at {@code t} is removed only after
 * {@code t + upperBound < watermark}. A right record is retained through the same horizon for
 * ordinary forward joins ({@code 0 <= lowerBound <= upperBound}); for a negative lower bound it
 * is retained through {@code max(upperBound, -lowerBound)}, because a future left record can be
 * as late as {@code rightTimestamp - lowerBound}. These deliberately strict comparisons retain
 * boundary matches when the watermark equals the last possible matching timestamp.
 *
 * <p>{@link Operator#onWatermark(long, Collector)} has no current key, so it cannot scan
 * keyed list state. Instead every buffered record registers a cleanup event-time timer. The
 * runtime restores that timer's key before {@link #onEventTimer(long, Object, Collector)};
 * the callback filters both current-key lists against the actual current watermark. Timers are
 * checkpointed by the runtime, so restored buffered records keep their scheduled cleanup.
 *
 * <p>The {@code left-buffer-size} and {@code right-buffer-size} gauges report buffers for keys
 * touched since this operator was opened. The API intentionally has no global keyed-state scan;
 * per-key bookkeeping lets the gauges reconstruct a restored key on its first record or timer
 * callback without treating normal multi-key traffic as a single key.
 *
 * @param <K> key type
 * @param <L> left input type
 * @param <R> right input type
 * @param <O> output type
 */
public final class IntervalJoinOperator<K, L, R, O>
        implements KeyedOperator<K, Either<L, R>, O> {

    private static final String LEFT_BUFFER_STATE = "interval-join-left";
    private static final String RIGHT_BUFFER_STATE = "interval-join-right";
    private static final String METRIC_ACCOUNTING_STATE = "interval-join-metric-accounting";

    private final long lowerBound;
    private final long upperBound;
    private final long rightRetention;
    private final JoinFunction<? super L, ? super R, ? extends O> joinFunction;

    private transient OperatorContext context;
    private transient ListState<TimestampedValue<L>> leftBuffer;
    private transient ListState<TimestampedValue<R>> rightBuffer;
    private transient ValueState<MetricAccounting> metricAccounting;
    private transient AtomicLong leftBufferSize;
    private transient AtomicLong rightBufferSize;
    private transient String metricEpoch;

    /**
     * Creates a join with millisecond bounds.
     *
     * @param lowerBound smallest allowed {@code rightTimestamp - leftTimestamp}, inclusive
     * @param upperBound largest allowed {@code rightTimestamp - leftTimestamp}, inclusive
     * @param joinFunction creates one output for each matching pair
     */
    public IntervalJoinOperator(long lowerBound, long upperBound,
                                JoinFunction<? super L, ? super R, ? extends O> joinFunction) {
        if (lowerBound > upperBound) {
            throw new IllegalArgumentException("lowerBound must not exceed upperBound");
        }
        if (lowerBound == Long.MIN_VALUE) {
            throw new IllegalArgumentException("lowerBound must be greater than Long.MIN_VALUE");
        }
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.rightRetention = Math.max(upperBound, -lowerBound);
        this.joinFunction = Objects.requireNonNull(joinFunction, "joinFunction");
    }

    /**
     * Creates a join with duration bounds.
     *
     * @param lowerBound smallest allowed {@code rightTimestamp - leftTimestamp}, inclusive
     * @param upperBound largest allowed {@code rightTimestamp - leftTimestamp}, inclusive
     * @param joinFunction creates one output for each matching pair
     */
    public IntervalJoinOperator(Duration lowerBound, Duration upperBound,
                                JoinFunction<? super L, ? super R, ? extends O> joinFunction) {
        this(Objects.requireNonNull(lowerBound, "lowerBound").toMillis(),
                Objects.requireNonNull(upperBound, "upperBound").toMillis(), joinFunction);
    }

    /** Returns the inclusive lower event-time offset in milliseconds. */
    public long lowerBound() {
        return lowerBound;
    }

    /** Returns the inclusive upper event-time offset in milliseconds. */
    public long upperBound() {
        return upperBound;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void open(OperatorContext context) {
        this.context = Objects.requireNonNull(context, "context");
        this.leftBuffer = context.getListState(LEFT_BUFFER_STATE, (Class<TimestampedValue<L>>) (Class<?>) TimestampedValue.class);
        this.rightBuffer = context.getListState(RIGHT_BUFFER_STATE, (Class<TimestampedValue<R>>) (Class<?>) TimestampedValue.class);
        this.metricAccounting = context.getValueState(METRIC_ACCOUNTING_STATE, MetricAccounting.class);
        this.leftBufferSize = new AtomicLong();
        this.rightBufferSize = new AtomicLong();
        this.metricEpoch = UUID.randomUUID().toString();
        context.metrics().gauge("left-buffer-size", leftBufferSize::get);
        context.metrics().gauge("right-buffer-size", rightBufferSize::get);
    }

    @Override
    public void processElement(StreamRecord<Either<L, R>> record, Collector<O> out)
            throws Exception {
        Objects.requireNonNull(record, "record");
        Objects.requireNonNull(out, "out");
        Either<L, R> tagged = Objects.requireNonNull(record.value(), "record.value");
        if (tagged.isLeft()) {
            L left = tagged.left();
            for (TimestampedValue<R> right : rightBuffer.get()) {
                if (isInInterval(record.timestamp(), right.timestamp())) {
                    out.collect(joinFunction.join(left, right.value()));
                }
            }
            leftBuffer.add(new TimestampedValue<>(left, record.timestamp()));
            registerCleanup(record.timestamp(), upperBound);
        } else {
            R right = tagged.right();
            for (TimestampedValue<L> left : leftBuffer.get()) {
                if (isInInterval(left.timestamp(), record.timestamp())) {
                    out.collect(joinFunction.join(left.value(), right));
                }
            }
            rightBuffer.add(new TimestampedValue<>(right, record.timestamp()));
            registerCleanup(record.timestamp(), rightRetention);
        }
        synchronizeGaugeForCurrentKey();
    }

    @Override
    public void onEventTimer(long timestamp, K key, Collector<O> out) throws Exception {
        // State is already scoped to key by the runtime. Do not use timestamp as the watermark:
        // a watermark may have leapt beyond several timers before this callback is entered.
        evictExpired(context.currentWatermark());
    }

    private boolean isInInterval(long leftTimestamp, long rightTimestamp) {
        // Compare without subtracting: right - left can overflow near the limits of long.
        return !isStrictlyBefore(rightTimestamp, -lowerBound, leftTimestamp)
                && !isStrictlyBefore(leftTimestamp, upperBound, rightTimestamp);
    }

    private void registerCleanup(long timestamp, long retention) {
        Long timer = timerAfterStrictExpiry(timestamp, retention);
        if (timer != null) {
            context.registerEventTimer(timer);
        }
    }

    private void evictExpired(long watermark) throws Exception {
        List<TimestampedValue<L>> retainedLeft = new ArrayList<>();
        for (TimestampedValue<L> left : leftBuffer.get()) {
            if (!isStrictlyBefore(left.timestamp(), upperBound, watermark)) {
                retainedLeft.add(left);
            }
        }
        List<TimestampedValue<R>> retainedRight = new ArrayList<>();
        for (TimestampedValue<R> right : rightBuffer.get()) {
            if (!isStrictlyBefore(right.timestamp(), rightRetention, watermark)) {
                retainedRight.add(right);
            }
        }
        leftBuffer.update(retainedLeft);
        rightBuffer.update(retainedRight);
        synchronizeGaugeForCurrentKey();
    }

    private void synchronizeGaugeForCurrentKey() throws Exception {
        int leftCount = count(leftBuffer.get());
        int rightCount = count(rightBuffer.get());
        MetricAccounting previous = metricAccounting.value().orElse(null);
        if (previous == null || !metricEpoch.equals(previous.epoch())) {
            leftBufferSize.addAndGet(leftCount);
            rightBufferSize.addAndGet(rightCount);
        } else {
            leftBufferSize.addAndGet(leftCount - previous.leftCount());
            rightBufferSize.addAndGet(rightCount - previous.rightCount());
        }
        if (leftCount == 0 && rightCount == 0) {
            metricAccounting.clear();
        } else {
            metricAccounting.update(new MetricAccounting(metricEpoch, leftCount, rightCount));
        }
    }

    private static int count(Iterable<?> values) {
        int count = 0;
        for (Object ignored : values) {
            count++;
        }
        return count;
    }

    /** Returns whether {@code timestamp + offset} is strictly before {@code boundary}. */
    private static boolean isStrictlyBefore(long timestamp, long offset, long boundary) {
        if (offset > 0 && timestamp > Long.MAX_VALUE - offset) {
            return false;
        }
        if (offset < 0 && timestamp < Long.MIN_VALUE - offset) {
            return true;
        }
        return timestamp + offset < boundary;
    }

    /**
     * Returns the first representable watermark that makes {@code timestamp + retention < wm}
     * true, or {@code null} when no such watermark can exist.
     */
    private static Long timerAfterStrictExpiry(long timestamp, long retention) {
        if (retention > 0 && timestamp > Long.MAX_VALUE - retention) {
            return null;
        }
        if (retention < 0 && timestamp < Long.MIN_VALUE - retention) {
            return Long.MIN_VALUE;
        }
        long expiry = timestamp + retention;
        return expiry == Long.MAX_VALUE ? null : expiry + 1;
    }

    private record TimestampedValue<T>(T value, long timestamp) implements Serializable {
        private TimestampedValue {
            Objects.requireNonNull(value, "value");
        }
    }

    private record MetricAccounting(String epoch, int leftCount, int rightCount)
            implements Serializable {
    }
}
