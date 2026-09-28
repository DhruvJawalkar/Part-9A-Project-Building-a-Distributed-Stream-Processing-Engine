package dev.dhruv.streaming.lms;

import org.junit.jupiter.api.Test;

import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Locks the field order and scalar types consumed by the Phase 6 table schemas.
 *
 * <p>These are deliberately model-level tests: the sink maps records to Iceberg rows, so a
 * renamed or reordered record component is a schema migration and must be reviewed explicitly.
 */
class LmsModelSchemaTest {

    @Test
    void browseSessionModelMatchesIcebergColumnContract() {
        assertThat(componentNames(SessionRow.class)).containsExactly(
                "memberId", "sessionStartMillis", "lastEventTimeMillis", "sessionEndMillis",
                "durationMillis", "clickCount", "searchTerms");
        assertThat(componentTypes(SessionRow.class)).containsExactly(
                String.class, long.class, long.class, long.class, long.class, long.class, List.class);
    }

    @Test
    void conversionModelMatchesIcebergColumnContract() {
        assertThat(componentNames(ConversionRow.class)).containsExactly(
                "memberId", "catalogItemId", "searchTerm", "loanId", "clickTimeMillis",
                "borrowTimeMillis", "conversionDelayMillis");
        assertThat(componentTypes(ConversionRow.class)).containsExactly(
                String.class, String.class, String.class, String.class,
                long.class, long.class, long.class);
    }

    @Test
    void sessionSearchTermsAreImmutableAtTheModelBoundary() {
        List<String> terms = new ArrayList<>(List.of("streams"));
        SessionRow row = new SessionRow("member-1", 1_000, 2_000, 902_000,
                1_000, 2, terms);

        terms.add("late mutation");

        assertThat(row.searchTerms()).containsExactly("streams");
        assertThatThrownBy(() -> row.searchTerms().add("not writable"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void conversionFactoryPreservesMillisecondEventTime() {
        ClickEvent click = new ClickEvent("member-1", "book-1", "streams",
                ClickEvent.RESULT_CLICK, 1_234);
        BorrowEvent borrow = new BorrowEvent("member-1", "book-1", "loan-1", 2_345);

        assertThat(ConversionRow.from(click, borrow)).isEqualTo(new ConversionRow(
                "member-1", "book-1", "streams", "loan-1", 1_234, 2_345, 1_111));
    }

    private static List<String> componentNames(Class<?> type) {
        return Arrays.stream(type.getRecordComponents())
                .map(RecordComponent::getName)
                .toList();
    }

    private static List<Class<?>> componentTypes(Class<?> type) {
        return Arrays.stream(type.getRecordComponents())
                .map(RecordComponent::getType)
                .toList();
    }
}
