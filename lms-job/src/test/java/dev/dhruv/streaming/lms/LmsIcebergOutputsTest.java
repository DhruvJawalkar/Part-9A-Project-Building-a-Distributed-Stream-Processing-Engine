package dev.dhruv.streaming.lms;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class LmsIcebergOutputsTest {

    @Test
    void mapsBrowseSessionsToTheExactTableColumnsWithUtcTimestamps() {
        SessionRow row = new SessionRow("member-1", 1_704_067_200_123L,
                1_704_067_201_456L, 1_704_068_100_789L, 1_333, 4,
                List.of("distributed systems", "iceberg"));

        assertThat(LmsIcebergOutputs.browseSessionFields(row)).isEqualTo(
                Map.<String, Object>of(
                        "member_id", "member-1",
                        "session_start", LocalDateTime.of(2024, 1, 1, 0, 0, 0, 123_000_000),
                        "last_event", LocalDateTime.of(2024, 1, 1, 0, 0, 1, 456_000_000),
                        "session_end", LocalDateTime.of(2024, 1, 1, 0, 15, 0, 789_000_000),
                        "duration_ms", 1_333L,
                        "click_count", 4L,
                        "search_terms", List.of("distributed systems", "iceberg")));
    }

    @Test
    void mapsConversionsToTheExactTableColumnsWithUtcTimestamps() {
        ConversionRow row = new ConversionRow("member-2", "book-3", "streaming", "loan-8",
                1_704_067_200_123L, 1_704_067_201_456L, 1_333);

        assertThat(LmsIcebergOutputs.clickConversionFields(row)).isEqualTo(
                Map.<String, Object>of(
                        "member_id", "member-2",
                        "catalog_item_id", "book-3",
                        "search_term", "streaming",
                        "loan_id", "loan-8",
                        "click_time", LocalDateTime.of(2024, 1, 1, 0, 0, 0, 123_000_000),
                        "borrow_time", LocalDateTime.of(2024, 1, 1, 0, 0, 1, 456_000_000),
                        "conversion_delay_ms", 1_333L));
    }

    @Test
    void readsCatalogAndS3SettingsFromEnvironmentWithLocalDemoDefaults() {
        LmsIcebergOutputs.IcebergCatalogSettings defaults =
                LmsIcebergOutputs.IcebergCatalogSettings.from(Map.of());
        LmsIcebergOutputs.IcebergCatalogSettings configured =
                LmsIcebergOutputs.IcebergCatalogSettings.from(Map.of(
                        "ICEBERG_CATALOG_NAME", "production",
                        "ICEBERG_CATALOG_URI", "https://catalog.example.test",
                        "ICEBERG_WAREHOUSE", "s3://production-warehouse/",
                        "ICEBERG_S3_ENDPOINT", "https://s3.example.test",
                        "ICEBERG_S3_ACCESS_KEY", "access",
                        "ICEBERG_S3_SECRET_KEY", "secret",
                        "ICEBERG_S3_REGION", "eu-west-1"));

        assertThat(defaults.catalogUri()).isEqualTo("http://localhost:8181");
        assertThat(defaults.warehouse()).isEqualTo("s3://warehouse/");
        assertThat(defaults.properties()).containsEntry("s3.endpoint", "http://localhost:9000")
                .containsEntry("s3.access-key-id", "minioadmin")
                .containsEntry("s3.secret-access-key", "minioadmin");
        assertThat(configured.catalogName()).isEqualTo("production");
        assertThat(configured.properties()).containsExactlyInAnyOrderEntriesOf(Map.of(
                "warehouse", "s3://production-warehouse/",
                "io-impl", "org.apache.iceberg.aws.s3.S3FileIO",
                "s3.endpoint", "https://s3.example.test",
                "s3.path-style-access", "true",
                "s3.access-key-id", "access",
                "s3.secret-access-key", "secret",
                "client.region", "eu-west-1"));
    }
}
