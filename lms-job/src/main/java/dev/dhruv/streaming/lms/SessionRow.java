package dev.dhruv.streaming.lms;

import java.io.Serializable;
import java.util.List;

/**
 * One completed catalog-browsing session for a member.
 *
 * <p>{@code sessionEndMillis} is the event-time instant at which the session is closed: the
 * last event plus the configured inactivity gap. It is deliberately distinct from
 * {@code lastEventTimeMillis}; {@code durationMillis} describes active browsing time, whereas
 * the session end is the timer timestamp used to decide that no more events can belong to it.
 *
 * @param memberId member whose keyed session closed
 * @param sessionStartMillis earliest event time in the session
 * @param lastEventTimeMillis latest event time in the session
 * @param sessionEndMillis event-time close instant, including the inactivity gap
 * @param durationMillis active duration from first to last event
 * @param clickCount number of clickstream events in the session
 * @param searchTerms distinct non-blank search terms, in deterministic lexical order
 */
public record SessionRow(
        String memberId,
        long sessionStartMillis,
        long lastEventTimeMillis,
        long sessionEndMillis,
        long durationMillis,
        long clickCount,
        List<String> searchTerms
) implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Defensively freezes the collection because rows cross task and process boundaries. */
    public SessionRow {
        searchTerms = List.copyOf(searchTerms);
    }
}
