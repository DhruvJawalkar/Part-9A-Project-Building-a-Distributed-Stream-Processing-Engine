package dev.dhruv.streaming.lms;

import dev.dhruv.streaming.api.Collector;
import dev.dhruv.streaming.api.KeyedOperator;
import dev.dhruv.streaming.api.OperatorContext;
import dev.dhruv.streaming.api.StreamRecord;
import dev.dhruv.streaming.api.metrics.Counter;
import dev.dhruv.streaming.api.state.ValueState;

import java.time.Duration;
import java.util.Objects;

/**
 * Builds one event-time session per member, closing it after fifteen minutes of inactivity.
 *
 * <p>Every event extends the session's candidate end to {@code event time + gap}. Timers are
 * intentionally not cancelled when a session extends: the public API keeps timer registration
 * small, so the callback compares its timestamp with the current end and ignores superseded
 * timers. The timer that matches clears both state values after emitting, preventing closed
 * sessions from accumulating one state entry per member forever. If an arriving event is beyond
 * the current session end, this operator closes the old session immediately and begins a new
 * one. An event more than one session gap before an open session's start cannot merge with it;
 * this teaching engine drops that late event and records it in {@code late-session-events} rather
 * than retaining historical sessions for a more elaborate merge.
 */
public final class SessionAggregator
        implements KeyedOperator<String, ClickEvent, SessionRow> {

    private static final long serialVersionUID = 1L;

    /** The business definition of one uninterrupted catalog-browsing session. */
    public static final Duration SESSION_GAP = Duration.ofMinutes(15);

    private final long allowedLatenessMillis;

    private transient OperatorContext context;
    private transient ValueState<SessionAccumulator> session;
    private transient ValueState<Long> windowEnd;
    private transient Counter lateSessionEvents;
    private transient Counter acceptedLateSessionEvents;

    /** Creates the normal policy: an event behind the current watermark is dropped. */
    public SessionAggregator() {
        this(Duration.ZERO);
    }

    /**
     * Creates a session operator which may revise an open session for this long behind the
     * watermark. The engine deliberately retains only the current open session, so an event
     * outside that session's gap is still dropped rather than recreating historical windows.
     */
    public SessionAggregator(Duration allowedLateness) {
        Objects.requireNonNull(allowedLateness, "allowedLateness");
        if (allowedLateness.isNegative()) {
            throw new IllegalArgumentException("allowed lateness must not be negative");
        }
        this.allowedLatenessMillis = allowedLateness.toMillis();
    }

    @Override
    public void open(OperatorContext context) {
        this.context = Objects.requireNonNull(context, "context");
        session = context.getValueState("session-accumulator", SessionAccumulator.class);
        windowEnd = context.getValueState("session-window-end", Long.class);
        lateSessionEvents = context.metrics().counter("late-session-events");
        acceptedLateSessionEvents = context.metrics().counter("accepted-late-session-events");
    }

    @Override
    public void processElement(StreamRecord<ClickEvent> record, Collector<SessionRow> out)
            throws Exception {
        Objects.requireNonNull(record, "record");
        ClickEvent event = Objects.requireNonNull(record.value(), "record.value");
        Long previousEnd = windowEnd.value().orElse(null);
        SessionAccumulator existing = session.value().orElse(null);
        boolean behindWatermark = isBehindWatermark(record.timestamp());
        if (behindWatermark && exceedsAllowedLateness(record.timestamp())) {
            lateSessionEvents.increment();
            return;
        }
        if (existing != null && isTooLateToMerge(record.timestamp(), existing)) {
            lateSessionEvents.increment();
            return;
        }
        if (behindWatermark) {
            acceptedLateSessionEvents.increment();
        }

        if (existing != null && previousEnd != null && record.timestamp() > previousEnd) {
            out.collect(existing.toRow(previousEnd), previousEnd);
            session.clear();
            windowEnd.clear();
            existing = null;
            previousEnd = null;
        }

        long newEnd = Math.addExact(record.timestamp(), SESSION_GAP.toMillis());
        SessionAccumulator accumulator = existing == null
                ? SessionAccumulator.empty(event, record.timestamp())
                : add(existing, event, record.timestamp());
        session.update(accumulator);

        if (previousEnd == null || newEnd > previousEnd) {
            windowEnd.update(newEnd);
            context.registerEventTimer(newEnd);
        }
    }

    @Override
    public void onEventTimer(long timestamp, String memberId, Collector<SessionRow> out)
            throws Exception {
        if (windowEnd.value().filter(end -> end == timestamp).isEmpty()) {
            return;
        }

        session.value().ifPresent(accumulator -> out.collect(accumulator.toRow(timestamp), timestamp));
        session.clear();
        windowEnd.clear();
    }

    private static SessionAccumulator add(SessionAccumulator accumulator, ClickEvent event,
                                          long eventTimeMillis) {
        accumulator.add(event, eventTimeMillis);
        return accumulator;
    }

    private static boolean isTooLateToMerge(long eventTimeMillis, SessionAccumulator existing) {
        return eventTimeMillis < Math.subtractExact(existing.sessionStartMillis(),
                SESSION_GAP.toMillis());
    }

    private boolean isBehindWatermark(long eventTimeMillis) {
        long watermark = context.currentWatermark();
        return watermark != Long.MIN_VALUE && eventTimeMillis < watermark;
    }

    private boolean exceedsAllowedLateness(long eventTimeMillis) {
        long watermark = context.currentWatermark();
        long cutoff = watermark < Long.MIN_VALUE + allowedLatenessMillis
                ? Long.MIN_VALUE : watermark - allowedLatenessMillis;
        return eventTimeMillis < cutoff;
    }
}
