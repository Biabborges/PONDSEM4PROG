
CREATE DATABASE IF NOT EXISTS default;

CREATE TABLE IF NOT EXISTS data_ingestion (
  timestamp_unix UInt64,
  data_value String,
  data_tag String
) ENGINE = MergeTree
ORDER BY timestamp_unix;

CREATE TABLE IF NOT EXISTS working_g1 (
  timestamp_unix UInt64,
  data_value String,
  data_tag String
) ENGINE = MergeTree
ORDER BY timestamp_unix;

CREATE TABLE IF NOT EXISTS working_g2 (
  timestamp_unix UInt64,
  data_value String,
  data_tag String
) ENGINE = MergeTree
ORDER BY timestamp_unix;

CREATE TABLE IF NOT EXISTS default.pipeline_metrics (
    execution_id UUID,
    batch_id String,
    pipeline_name String,
    execution_timestamp DateTime,
    rows_processed UInt64,
    rows_valid UInt64,
    rows_invalid UInt64,
    duplicate_rows UInt64,
    null_counts Map(String, UInt64),
    status String,
    status_message String
) ENGINE = MergeTree()
ORDER BY execution_timestamp;
