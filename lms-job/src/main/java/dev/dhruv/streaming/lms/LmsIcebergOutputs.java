package dev.dhruv.streaming.lms;

import dev.dhruv.streaming.connectors.iceberg.IcebergSink;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

/**
 * The LMS-specific contract between its output records and the two Iceberg tables.
 *
 * <p>The connector deliberately accepts ordinary named values, so this class can keep the
 * table's physical column names and timestamp representation close to the LMS model without
 * exposing Iceberg implementation types to job code.
 */
final class LmsIcebergOutputs {

    static final String BROWSE_SESSIONS_TABLE = "lms.analytics.browse_sessions";
    static final String CLICK_CONVERSIONS_TABLE = "lms.analytics.click_conversions";

    private LmsIcebergOutputs() {
    }

    static IcebergSink<SessionRow> browseSessionsSink() {
        IcebergCatalogSettings settings = IcebergCatalogSettings.fromEnvironment();
        return IcebergSink.forRestCatalog(settings.catalogName(), settings.catalogUri(),
                BROWSE_SESSIONS_TABLE, settings.properties(), LmsIcebergOutputs::browseSessionFields,
                "browse-sessions");
    }

    static IcebergSink<ConversionRow> clickConversionsSink() {
        IcebergCatalogSettings settings = IcebergCatalogSettings.fromEnvironment();
        return IcebergSink.forRestCatalog(settings.catalogName(), settings.catalogUri(),
                CLICK_CONVERSIONS_TABLE, settings.properties(),
                LmsIcebergOutputs::clickConversionFields, "click-conversions");
    }

    static Map<String, ?> browseSessionFields(SessionRow row) {
        return Map.of(
                "member_id", row.memberId(),
                "session_start", utcTimestamp(row.sessionStartMillis()),
                "last_event", utcTimestamp(row.lastEventTimeMillis()),
                "session_end", utcTimestamp(row.sessionEndMillis()),
                "duration_ms", row.durationMillis(),
                "click_count", row.clickCount(),
                "search_terms", List.copyOf(row.searchTerms()));
    }

    static Map<String, ?> clickConversionFields(ConversionRow row) {
        return Map.of(
                "member_id", row.memberId(),
                "catalog_item_id", row.catalogItemId(),
                "search_term", row.searchTerm(),
                "loan_id", row.loanId(),
                "click_time", utcTimestamp(row.clickTimeMillis()),
                "borrow_time", utcTimestamp(row.borrowTimeMillis()),
                "conversion_delay_ms", row.conversionDelayMillis());
    }

    /** Iceberg's timestamp (without zone) is represented by a UTC {@link LocalDateTime}. */
    static LocalDateTime utcTimestamp(long epochMillis) {
        return Instant.ofEpochMilli(epochMillis).atOffset(ZoneOffset.UTC).toLocalDateTime();
    }

    /**
     * Serializable REST catalog settings. Defaults target the repository's local Compose demo.
     */
    record IcebergCatalogSettings(String catalogName, String catalogUri, String warehouse,
            String s3Endpoint, String s3AccessKey, String s3SecretKey, String s3Region) {

        private static final String DEFAULT_CATALOG_NAME = "lms";
        private static final String DEFAULT_CATALOG_URI = "http://localhost:8181";
        private static final String DEFAULT_WAREHOUSE = "s3://warehouse/";
        private static final String DEFAULT_S3_ENDPOINT = "http://localhost:9000";
        private static final String DEFAULT_S3_ACCESS_KEY = "minioadmin";
        private static final String DEFAULT_S3_SECRET_KEY = "minioadmin";
        private static final String DEFAULT_S3_REGION = "us-east-1";

        static IcebergCatalogSettings fromEnvironment() {
            return from(System.getenv());
        }

        static IcebergCatalogSettings from(Map<String, String> environment) {
            return new IcebergCatalogSettings(
                    first(environment, DEFAULT_CATALOG_NAME, "ICEBERG_CATALOG_NAME"),
                    first(environment, DEFAULT_CATALOG_URI, "ICEBERG_CATALOG_URI"),
                    first(environment, DEFAULT_WAREHOUSE, "ICEBERG_WAREHOUSE"),
                    first(environment, DEFAULT_S3_ENDPOINT, "ICEBERG_S3_ENDPOINT"),
                    first(environment, DEFAULT_S3_ACCESS_KEY, "ICEBERG_S3_ACCESS_KEY",
                            "ICEBERG_S3_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID", "MINIO_ACCESS_KEY"),
                    first(environment, DEFAULT_S3_SECRET_KEY, "ICEBERG_S3_SECRET_KEY",
                            "ICEBERG_S3_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY",
                            "MINIO_SECRET_KEY"),
                    first(environment, DEFAULT_S3_REGION, "ICEBERG_S3_REGION", "AWS_REGION"));
        }

        Map<String, String> properties() {
            return Map.of(
                    "warehouse", warehouse,
                    "io-impl", "org.apache.iceberg.aws.s3.S3FileIO",
                    "s3.endpoint", s3Endpoint,
                    "s3.path-style-access", "true",
                    "s3.access-key-id", s3AccessKey,
                    "s3.secret-access-key", s3SecretKey,
                    "client.region", s3Region);
        }

        private static String first(Map<String, String> environment, String defaultValue,
                String... names) {
            for (String name : names) {
                String value = environment.get(name);
                if (value != null && !value.isBlank()) {
                    return value;
                }
            }
            return defaultValue;
        }
    }
}
