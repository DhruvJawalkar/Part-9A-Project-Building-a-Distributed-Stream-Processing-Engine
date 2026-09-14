package dev.dhruv.streaming.lms;

/**
 * A member interacting with the library catalog: searching, or clicking a search result.
 *
 * <p>Published to the {@code lms.catalog.clicks} topic, partitioned by {@code memberId} so that
 * a member's events stay in order relative to one another.
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
) {

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
