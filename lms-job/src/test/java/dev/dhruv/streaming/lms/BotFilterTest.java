package dev.dhruv.streaming.lms;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the bot filter.
 *
 * <p>A stateless operator is the easiest thing in the engine to test, and that is worth
 * noticing rather than glossing over: no engine, no threads, no fixtures. Just a function.
 * Every bit of that ease comes from the operator holding no state and touching no time, which
 * is exactly what stops being true one phase from now.
 */
class BotFilterTest {

    private final BotFilter filter = new BotFilter();

    @Test
    @DisplayName("keeps an ordinary member click")
    void keepsRealMember() throws Exception {
        assertThat(filter.keep(click("m-1001", 1757836800000L))).isTrue();
    }

    @Test
    @DisplayName("drops the catalog crawler")
    void dropsCrawler() throws Exception {
        assertThat(filter.keep(click("bot-crawler-3", 1757836800000L))).isFalse();
    }

    @Test
    @DisplayName("drops load-test traffic")
    void dropsLoadTest() throws Exception {
        assertThat(filter.keep(click("bot-loadtest-9", 1757836800000L))).isFalse();
    }

    @Test
    @DisplayName("drops an event with no member id")
    void dropsMissingMemberId() throws Exception {
        assertThat(filter.keep(click("", 1757836800000L))).isFalse();
        assertThat(filter.keep(click("   ", 1757836800000L))).isFalse();
        assertThat(filter.keep(click(null, 1757836800000L))).isFalse();
    }

    @Test
    @DisplayName("drops an event with no event time")
    void dropsMissingEventTime() throws Exception {
        // An event that cannot be placed on the event-time axis cannot contribute to any answer
        // this job produces. Better to drop it here than to let it distort a session boundary
        // downstream, where it would be much harder to trace.
        assertThat(filter.keep(click("m-1001", 0L))).isFalse();
        assertThat(filter.keep(click("m-1001", -1L))).isFalse();
    }

    @Test
    @DisplayName("drops a null event rather than throwing")
    void dropsNull() throws Exception {
        // A malformed message reaches here as null rather than as an exception, because one bad
        // record must not fail the task. A failing task would restart, meet the same record,
        // and fail again.
        assertThat(filter.keep(null)).isFalse();
    }

    @Test
    @DisplayName("keeps a member whose id merely contains 'bot'")
    void onlyPrefixCounts() throws Exception {
        // "robot-fan-22" is a person with a username, not a crawler. The check is a prefix for
        // a reason.
        assertThat(filter.keep(click("robot-fan-22", 1757836800000L))).isTrue();
    }

    private static ClickEvent click(String memberId, long eventTimeMillis) {
        return new ClickEvent(memberId, "cat-7741", "distributed systems",
                ClickEvent.RESULT_CLICK, eventTimeMillis);
    }
}
