
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
