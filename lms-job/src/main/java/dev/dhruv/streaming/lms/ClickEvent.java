package dev.dhruv.streaming.lms;

import java.io.Serializable;

/**
 * A member interacting with the library catalog: searching, or clicking a search result.
 *
 * <p>Published to the {@code lms.catalog.clicks} topic, partitioned by {@code memberId} so that
 * a member's events stay in order relative to one another.
 *
 * <p>Serializable because it travels between processes. A record moving from a filter on one
 * worker to a session aggregator on another is serialized, sent and rebuilt, and Java records are
 * not serializable unless they say so. This is the one obligation the engine places on a job
 * author's own types, and it is worth stating plainly: whatever flows between operators has to be
 * able to leave the JVM it was created in.
 *
 * @param memberId        who did it; the partitioning key and the key all session state is
 *                        scoped to
 * @param catalogItemId   which catalog item was clicked; empty for a bare search
 * @param searchTerm      what was typed
 * @param eventType       {@code SEARCH} or {@code RESULT_CLICK}
 * @param eventTimeMillis when it happened, stamped by the client rather than on arrival. This
 *                        is the field the whole engine reasons about: it is why a replay of
 *                        yesterday's events produces yesterday's answers rather than today's.
 */
public record ClickEvent(
        String memberId,
        String catalogItemId,
        String searchTerm,
        String eventType,
        long eventTimeMillis
) implements Serializable {

    private static final long serialVersionUID = 1L;

    /** A member typed a query. */
    public static final String SEARCH = "SEARCH";

    /** A member clicked through to an item from the results. */
    public static final String RESULT_CLICK = "RESULT_CLICK";

    /**
     * Returns whether this event is a click through to a catalog item, as opposed to a bare
     * search. Only these are candidates for conversion into a borrow.
     *
     * @return true if this is a result click
     */
    public boolean isResultClick() {
        return RESULT_CLICK.equals(eventType);
    }
}
