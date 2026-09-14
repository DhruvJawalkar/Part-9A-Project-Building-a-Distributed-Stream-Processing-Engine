package dev.dhruv.streaming.lms;

import dev.dhruv.streaming.api.FilterFunction;

/**
 * Drops click events that are malformed or that came from a crawler rather than a member.
 *
 * <p>Stateless, and worth noticing how much follows from that. It needs no key, so it can run
 * at any parallelism; it needs no state, so a checkpoint of it is empty and recovering it is
 * free; and it sits behind a forward exchange, so the engine can fuse it into the same thread
 * as its neighbour and skip serializing anything between them.
 *
 * <p>Filtering early is the cheapest optimisation in any streaming job. Every record dropped
 * here is one that never gets serialized, never crosses a process boundary, and never occupies
 * a byte of session state downstream.
 */
public final class BotFilter implements FilterFunction<ClickEvent> {

    private static final long serialVersionUID = 1L;

    /** Member id prefix used by the catalog crawler and by load-test traffic. */
    private static final String BOT_PREFIX = "bot-";

    @Override
    public boolean keep(ClickEvent event) {
        if (event == null || event.memberId() == null || event.memberId().isBlank()) {
            return false;
        }
        if (event.memberId().startsWith(BOT_PREFIX)) {
            return false;
        }
        // An event with no event time cannot be placed in a window, so it cannot contribute to
        // any answer this job produces. Dropping it here is better than letting it reach the
        // session operator and quietly distort a session's boundaries.
        return event.eventTimeMillis() > 0;
    }
}
