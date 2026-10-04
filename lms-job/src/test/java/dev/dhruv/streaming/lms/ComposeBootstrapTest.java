package dev.dhruv.streaming.lms;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ComposeBootstrapTest {

    @Test
    void reusesRunningOrRecoveringGraphButAllowsFreshSubmissionAfterTerminalState() throws Exception {
        for (String state : new String[] {"CREATED", "RUNNING", "FAILING", "RESTARTING"}) {
            assertThat(SubmitLmsJob.reusableJob("[{\"name\":\"lms-clickstream\",\"jobId\":\"existing\","
                    + "\"state\":\"" + state + "\"}]")).contains("existing");
        }
        assertThat(SubmitLmsJob.reusableJob("[{\"name\":\"lms-clickstream\",\"jobId\":\"old\","
                + "\"state\":\"FAILED\"},{\"name\":\"other-job\",\"state\":\"RUNNING\"}]"))
                .isEmpty();
    }

    @Test
    void failsClosedOnAmbiguousOrMalformedJobStatus() {
        assertThatThrownBy(() -> SubmitLmsJob.reusableJob("{}"))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("JSON array");
        assertThatThrownBy(() -> SubmitLmsJob.reusableJob("[{\"name\":\"lms-clickstream\","
                + "\"state\":\"RUNNING\"}]"))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("without a jobId");
        assertThatThrownBy(() -> SubmitLmsJob.reusableJob("[{\"name\":\"lms-clickstream\","
                + "\"jobId\":\"one\",\"state\":\"RUNNING\"},{\"name\":\"lms-clickstream\","
                + "\"jobId\":\"two\",\"state\":\"RESTARTING\"}]"))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("multiple live");
    }

    @Test
    void emptyTopicsSeedOnceAndCompleteOrExtendedFixturesSkipReplay() {
        TopicPartition clicks = new TopicPartition("lms.catalog.clicks", 0);
        TopicPartition borrows = new TopicPartition("lms.catalog.borrows", 0);
        Map<TopicPartition, Long> baseline = Map.of(clicks, 3L, borrows, 2L);
        assertThat(PublishLmsFixture.shouldPublishBootstrap(Map.of(clicks, 0L, borrows, 0L), baseline))
                .isTrue();
        assertThat(PublishLmsFixture.shouldPublishBootstrap(baseline, baseline)).isFalse();
        assertThat(PublishLmsFixture.shouldPublishBootstrap(Map.of(clicks, 7L, borrows, 6L), baseline))
                .isFalse();
    }

    @Test
    void partialPublicationFailsEvenWhenBothTopicsAlreadyContainSomeRecords() {
        TopicPartition clicks = new TopicPartition("lms.catalog.clicks", 0);
        TopicPartition borrows = new TopicPartition("lms.catalog.borrows", 0);
        TopicPartition borrowSecond = new TopicPartition("lms.catalog.borrows", 1);
        Map<TopicPartition, Long> baseline = Map.of(clicks, 3L, borrows, 2L, borrowSecond, 2L);
        for (Map<TopicPartition, Long> partial : List.of(
                Map.of(clicks, 3L, borrows, 0L, borrowSecond, 0L),
                Map.of(clicks, 3L, borrows, 1L, borrowSecond, 2L),
                Map.of(clicks, 9L, borrows, 9L, borrowSecond, 1L))) {
            assertThatThrownBy(() -> PublishLmsFixture.shouldPublishBootstrap(partial, baseline))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("partial or incompatible")
                    .hasMessageContaining("automatic replay would duplicate");
        }
    }
}
