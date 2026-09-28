package dev.dhruv.streaming.lms;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;

/**
 * The incremental state held for one open session.
 *
 * <p>This is a running aggregate, deliberately not a list of {@link ClickEvent}s. Its memory
 * stays bounded by the number of distinct search terms, rather than growing with every event
 * a busy member generates before their session closes.
 */
final class SessionAccumulator implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String memberId;
    private long sessionStartMillis;
    private long lastEventTimeMillis;
    private long clickCount;
    private final TreeSet<String> searchTerms;

    private SessionAccumulator(String memberId, long eventTimeMillis) {
        this.memberId = Objects.requireNonNull(memberId, "memberId");
        this.sessionStartMillis = eventTimeMillis;
        this.lastEventTimeMillis = eventTimeMillis;
        this.searchTerms = new TreeSet<>();
    }

    static SessionAccumulator empty(ClickEvent firstEvent, long eventTimeMillis) {
        SessionAccumulator accumulator = new SessionAccumulator(firstEvent.memberId(), eventTimeMillis);
        accumulator.add(firstEvent, eventTimeMillis);
        return accumulator;
    }

    /** Rehydrates an already-closed local fragment for the global aggregation phase. */
    static SessionAccumulator from(SaltedSessionAggregator.SessionSegment segment) {
        Objects.requireNonNull(segment, "segment");
        SessionAccumulator accumulator = new SessionAccumulator(segment.memberId(),
                segment.sessionStartMillis());
        accumulator.lastEventTimeMillis = segment.lastEventTimeMillis();
        accumulator.clickCount = segment.clickCount();
        accumulator.searchTerms.addAll(segment.searchTerms());
        return accumulator;
    }

    void add(ClickEvent event, long eventTimeMillis) {
        Objects.requireNonNull(event, "event");
        if (!memberId.equals(event.memberId())) {
            throw new IllegalArgumentException("a session may only contain one member's events");
        }
        sessionStartMillis = Math.min(sessionStartMillis, eventTimeMillis);
        lastEventTimeMillis = Math.max(lastEventTimeMillis, eventTimeMillis);
        clickCount++;
        if (event.searchTerm() != null && !event.searchTerm().isBlank()) {
            searchTerms.add(event.searchTerm());
        }
    }

    /** Combines a closed local fragment without retaining its individual input events. */
    void merge(SaltedSessionAggregator.SessionSegment segment) {
        Objects.requireNonNull(segment, "segment");
        if (!memberId.equals(segment.memberId())) {
            throw new IllegalArgumentException("a session may only contain one member's events");
        }
        sessionStartMillis = Math.min(sessionStartMillis, segment.sessionStartMillis());
        lastEventTimeMillis = Math.max(lastEventTimeMillis, segment.lastEventTimeMillis());
        clickCount = Math.addExact(clickCount, segment.clickCount());
        searchTerms.addAll(segment.searchTerms());
    }

    SessionRow toRow(long sessionEndMillis) {
        return new SessionRow(memberId, sessionStartMillis, lastEventTimeMillis, sessionEndMillis,
                lastEventTimeMillis - sessionStartMillis, clickCount, List.copyOf(searchTerms));
    }

    /** Event time of the first member action retained by this open session. */
    long sessionStartMillis() {
        return sessionStartMillis;
    }

    long lastEventTimeMillis() {
        return lastEventTimeMillis;
    }
}
