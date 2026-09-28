package dev.dhruv.streaming.lms;

import java.io.Serializable;

/**
 * One catalog result click that converted into a borrow inside the configured interval.
 *
 * @param memberId             member who clicked and borrowed
 * @param catalogItemId        catalog item involved
 * @param searchTerm           query which led to the result click
 * @param loanId               lending-system record created by the borrow
 * @param clickTimeMillis      click event time
 * @param borrowTimeMillis     borrow event time
 * @param conversionDelayMillis elapsed event time from click to borrow
 */
public record ConversionRow(
        String memberId,
        String catalogItemId,
        String searchTerm,
        String loanId,
        long clickTimeMillis,
        long borrowTimeMillis,
        long conversionDelayMillis
) implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Creates the business row emitted by the interval join. */
    public static ConversionRow from(ClickEvent click, BorrowEvent borrow) {
        if (!click.memberId().equals(borrow.memberId())
                || !click.catalogItemId().equals(borrow.catalogItemId())) {
            throw new IllegalArgumentException("a conversion requires the same member and item");
        }
        return new ConversionRow(click.memberId(), click.catalogItemId(), click.searchTerm(),
                borrow.loanId(), click.eventTimeMillis(), borrow.eventTimeMillis(),
                borrow.eventTimeMillis() - click.eventTimeMillis());
    }
}
