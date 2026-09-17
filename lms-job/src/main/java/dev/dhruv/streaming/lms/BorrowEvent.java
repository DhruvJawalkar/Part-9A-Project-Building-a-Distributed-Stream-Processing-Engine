package dev.dhruv.streaming.lms;

import java.io.Serializable;

/**
 * A member borrowing a catalog item.
 *
 * <p>Published to the {@code lms.catalog.borrows} topic, partitioned by {@code memberId}. Phase
 * 5 joins these against clicks to answer which searches actually led somewhere.
 *
 * <p>Serializable for the same reason as {@link ClickEvent}: it crosses process boundaries.
 *
 * @param memberId        who borrowed it
 * @param catalogItemId   what they borrowed
 * @param loanId          the loan record created
 * @param eventTimeMillis when it happened, stamped by the client
 */
public record BorrowEvent(
        String memberId,
        String catalogItemId,
        String loanId,
        long eventTimeMillis
) implements Serializable {

    private static final long serialVersionUID = 1L;
}
