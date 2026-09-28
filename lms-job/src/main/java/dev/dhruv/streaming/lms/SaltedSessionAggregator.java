package dev.dhruv.streaming.lms;

import dev.dhruv.streaming.api.Collector;
import dev.dhruv.streaming.api.KeyedOperator;
import dev.dhruv.streaming.api.Operator;
import dev.dhruv.streaming.api.OperatorContext;
import dev.dhruv.streaming.api.StreamRecord;
import dev.dhruv.streaming.api.metrics.Counter;
import dev.dhruv.streaming.api.state.ValueState;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * The two-stage session path used when one member is much hotter than the rest of the keyspace.
 *
 * <p>The first stage hashes each event to one of {@link #SALT_BUCKETS} stable member shards and
 * builds compact, closed local fragments. The second stage hashes those fragments back by member
 * and merges them into the normal {@link SessionRow}. Thus no individual local shard owns every
 * click for a hot member, while exactly one global key owns the small number of closed fragments.
 *
 * <p>Closing globally needs one extra session gap. A local fragment ending at {@code t} can be
 * followed by a fragment from another salt whose last event is as late as {@code t}; waiting
 * through the additional gap makes that fragment event-time complete before the global session
 * is published. The output row retains its business end ({@code last event + gap}); only its
 * availability is delayed. This is the price that keeps arbitrary salt assignment from changing
 * the definition of a session.
 */
public final class SaltedSessionAggregator {

    /** Enough shards to reveal and relieve a single hot member without an excessive fan-out. */
    public static final int SALT_BUCKETS = 16;

    private static final long SESSION_GAP_MILLIS = SessionAggregator.SESSION_GAP.toMillis();

    private SaltedSessionAggregator() {
    }

    /**
     * Produces a deterministic salt from the complete event payload.
     *
     * <p>It deliberately has no counter: the sender and receiver may independently replay an
     * edge after a checkpoint, so the routing key must be a pure function of serialised data.
     * The event schema has no immutable event id; exactly duplicate payloads therefore share a
     * shard, which is correct for replay and still distributes the usual time-varying hot flow.
     */
    public static int saltFor(ClickEvent event) {
        Objects.requireNonNull(event, "event");
        long mixed = 0x9e3779b97f4a7c15L;
        mixed = mix(mixed ^ event.memberId().hashCode());
        mixed = mix(mixed ^ Objects.hashCode(event.catalogItemId()));
        mixed = mix(mixed ^ Objects.hashCode(event.searchTerm()));
        mixed = mix(mixed ^ Objects.hashCode(event.eventType()));
        mixed = mix(mixed ^ event.eventTimeMillis());
        return Math.floorMod((int) (mixed ^ (mixed >>> 32)), SALT_BUCKETS);
    }

    private static long mix(long value) {
        value = (value ^ (value >>> 30)) * 0xbf58476d1ce4e5b9L;
        value = (value ^ (value >>> 27)) * 0x94d049bb133111ebL;
        return value ^ (value >>> 31);
    }

    /** Turns raw clicks into stable, explicitly inspectable salted keys. */
    public static final class Salter implements Operator<ClickEvent, SaltedClickEvent> {
        private static final long serialVersionUID = 1L;

        @Override
        public void processElement(StreamRecord<ClickEvent> record, Collector<SaltedClickEvent> out) {
            ClickEvent event = Objects.requireNonNull(record.value(), "record.value");
            out.collect(new SaltedClickEvent(event, saltFor(event)), record.timestamp());
        }
    }

    /** First phase: one ordinary incremental session aggregate per {@code (member, salt)}. */
    public static final class LocalSessionAggregator
            implements KeyedOperator<SaltedMemberKey, SaltedClickEvent, SessionSegment> {

        private static final long serialVersionUID = 1L;

        private transient OperatorContext context;
        private transient ValueState<SessionAccumulator> session;
        private transient ValueState<Long> windowEnd;
        private transient Counter lateSessionEvents;

        @Override
        public void open(OperatorContext context) {
            this.context = Objects.requireNonNull(context, "context");
            session = context.getValueState("local-session-accumulator", SessionAccumulator.class);
            windowEnd = context.getValueState("local-session-window-end", Long.class);
            lateSessionEvents = context.metrics().counter("late-local-session-events");
        }

        @Override
        public void processElement(StreamRecord<SaltedClickEvent> record, Collector<SessionSegment> out)
                throws Exception {
            SaltedClickEvent salted = Objects.requireNonNull(record.value(), "record.value");
            ClickEvent event = salted.event();
            Long previousEnd = windowEnd.value().orElse(null);
            SessionAccumulator existing = session.value().orElse(null);
            if (existing != null && isTooLateToMerge(record.timestamp(), existing)) {
                lateSessionEvents.increment();
                return;
            }
            if (existing != null && previousEnd != null && record.timestamp() > previousEnd) {
                out.collect(SessionSegment.from(existing, previousEnd), previousEnd);
                session.clear();
                windowEnd.clear();
                existing = null;
                previousEnd = null;
            }

            long newEnd = Math.addExact(record.timestamp(), SESSION_GAP_MILLIS);
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
        public void onEventTimer(long timestamp, SaltedMemberKey key, Collector<SessionSegment> out)
                throws Exception {
            if (windowEnd.value().filter(end -> end == timestamp).isEmpty()) {
                return;
            }
            session.value().ifPresent(accumulator ->
                    out.collect(SessionSegment.from(accumulator, timestamp), timestamp));
            session.clear();
            windowEnd.clear();
        }
    }

    /** Second phase: merges the compact fragments back to one exact member session. */
    public static final class GlobalSessionAggregator
            implements KeyedOperator<String, SessionSegment, SessionRow> {

        private static final long serialVersionUID = 1L;

        private transient OperatorContext context;
        private transient ValueState<SessionAccumulator> session;
        private transient ValueState<Long> businessEnd;
        private transient ValueState<Long> settledAt;
        private transient Counter lateSessionFragments;

        @Override
        public void open(OperatorContext context) {
            this.context = Objects.requireNonNull(context, "context");
            session = context.getValueState("global-session-accumulator", SessionAccumulator.class);
            businessEnd = context.getValueState("global-session-business-end", Long.class);
            settledAt = context.getValueState("global-session-settled-at", Long.class);
            lateSessionFragments = context.metrics().counter("late-global-session-fragments");
        }

        @Override
        public void processElement(StreamRecord<SessionSegment> record, Collector<SessionRow> out)
                throws Exception {
            SessionSegment fragment = Objects.requireNonNull(record.value(), "record.value");
            SessionAccumulator existing = session.value().orElse(null);
            Long previousBusinessEnd = businessEnd.value().orElse(null);
            if (existing != null && isTooLateToMerge(fragment.lastEventTimeMillis(), existing)) {
                lateSessionFragments.increment();
                return;
            }
            if (existing != null && previousBusinessEnd != null
                    && fragment.sessionStartMillis() > previousBusinessEnd) {
                out.collect(existing.toRow(previousBusinessEnd), previousBusinessEnd);
                session.clear();
                businessEnd.clear();
                settledAt.clear();
                existing = null;
            }

            SessionAccumulator accumulator = existing == null
                    ? SessionAccumulator.from(fragment)
                    : merge(existing, fragment);
            long newBusinessEnd = Math.addExact(accumulator.lastEventTimeMillis(),
                    SESSION_GAP_MILLIS);
            long newSettledAt = Math.addExact(newBusinessEnd, SESSION_GAP_MILLIS);
            session.update(accumulator);
            businessEnd.update(newBusinessEnd);
            Long previousSettledAt = settledAt.value().orElse(null);
            if (previousSettledAt == null || newSettledAt > previousSettledAt) {
                settledAt.update(newSettledAt);
                context.registerEventTimer(newSettledAt);
            }
        }

        @Override
        public void onEventTimer(long timestamp, String memberId, Collector<SessionRow> out)
                throws Exception {
            if (settledAt.value().filter(end -> end == timestamp).isEmpty()) {
                return;
            }
            Long end = businessEnd.value().orElseThrow();
            session.value().ifPresent(accumulator -> out.collect(accumulator.toRow(end), end));
            session.clear();
            businessEnd.clear();
            settledAt.clear();
        }
    }

    private static SessionAccumulator add(SessionAccumulator accumulator, ClickEvent event,
                                          long eventTimeMillis) {
        accumulator.add(event, eventTimeMillis);
        return accumulator;
    }

    private static SessionAccumulator merge(SessionAccumulator accumulator, SessionSegment fragment) {
        accumulator.merge(fragment);
        return accumulator;
    }

    private static boolean isTooLateToMerge(long eventTimeMillis, SessionAccumulator existing) {
        return eventTimeMillis < Math.subtractExact(existing.sessionStartMillis(),
                SESSION_GAP_MILLIS);
    }

    /** A stable hash key for the local phase. */
    public record SaltedMemberKey(String memberId, int salt) implements Serializable {
        private static final long serialVersionUID = 1L;

        public SaltedMemberKey {
            Objects.requireNonNull(memberId, "memberId");
            if (salt < 0 || salt >= SALT_BUCKETS) {
                throw new IllegalArgumentException("salt must be between 0 and "
                        + (SALT_BUCKETS - 1));
            }
        }
    }

    /** A closed, serializable local fragment. It carries aggregates, never an event list. */
    public record SessionSegment(
            String memberId,
            long sessionStartMillis,
            long lastEventTimeMillis,
            long clickCount,
            List<String> searchTerms
    ) implements Serializable {
        private static final long serialVersionUID = 1L;

        public SessionSegment {
            Objects.requireNonNull(memberId, "memberId");
            searchTerms = List.copyOf(searchTerms);
        }

        static SessionSegment from(SessionAccumulator accumulator, long sessionEndMillis) {
            SessionRow row = accumulator.toRow(sessionEndMillis);
            return new SessionSegment(row.memberId(), row.sessionStartMillis(),
                    row.lastEventTimeMillis(), row.clickCount(), row.searchTerms());
        }
    }

    /** The value emitted by {@link Salter}; key extraction stays visible at the graph boundary. */
    public record SaltedClickEvent(ClickEvent event, int salt) implements Serializable {
        private static final long serialVersionUID = 1L;

        public SaltedClickEvent {
            Objects.requireNonNull(event, "event");
            if (salt < 0 || salt >= SALT_BUCKETS) {
                throw new IllegalArgumentException("salt must be between 0 and "
                        + (SALT_BUCKETS - 1));
            }
        }

        public SaltedMemberKey key() {
            return new SaltedMemberKey(event.memberId(), salt);
        }
    }
}
