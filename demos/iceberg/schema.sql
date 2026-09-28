-- Phase 6 LMS output tables.
--
-- This is the canonical human-readable schema contract. Run it with a SQL engine configured
-- with an Iceberg REST catalog (for example Spark SQL or Trino); init-schema.sh performs the
-- equivalent REST calls without requiring a second SQL engine in the demo stack.

CREATE NAMESPACE IF NOT EXISTS lms;
CREATE NAMESPACE IF NOT EXISTS lms.analytics;

CREATE TABLE IF NOT EXISTS lms.analytics.browse_sessions (
    member_id STRING NOT NULL,
    session_start TIMESTAMP(3) NOT NULL,
    last_event TIMESTAMP(3) NOT NULL,
    session_end TIMESTAMP(3) NOT NULL,
    duration_ms BIGINT NOT NULL,
    click_count BIGINT NOT NULL,
    search_terms ARRAY<STRING> NOT NULL
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet'
);

CREATE TABLE IF NOT EXISTS lms.analytics.click_conversions (
    member_id STRING NOT NULL,
    catalog_item_id STRING NOT NULL,
    search_term STRING NOT NULL,
    loan_id STRING NOT NULL,
    click_time TIMESTAMP(3) NOT NULL,
    borrow_time TIMESTAMP(3) NOT NULL,
    conversion_delay_ms BIGINT NOT NULL
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.format.default' = 'parquet'
);
