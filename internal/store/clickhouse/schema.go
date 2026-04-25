package clickhouse

const createMetadataTableSQL = `
CREATE TABLE IF NOT EXISTS otel_metrics_metadata (
    Fingerprint UInt64 CODEC(ZSTD(1)),
    MetricType  Enum8('gauge'=1, 'sum'=2) CODEC(ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    MetricName  LowCardinality(String) CODEC(ZSTD(1)),
    MetricDescription String CODEC(ZSTD(1)),
    MetricUnit  LowCardinality(String) CODEC(ZSTD(1)),
    ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ResourceSchemaUrl  String CODEC(ZSTD(1)),
    ScopeName    LowCardinality(String) CODEC(ZSTD(1)),
    ScopeVersion LowCardinality(String) CODEC(ZSTD(1)),
    ScopeAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeDroppedAttrCount UInt32 CODEC(ZSTD(1)),
    ScopeSchemaUrl String CODEC(ZSTD(1)),
    Attributes   Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    AggregationTemporality Int32 CODEC(ZSTD(1)),
    IsMonotonic  Bool CODEC(ZSTD(1)),
    FirstSeen    DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    LastSeen     DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    INDEX idx_service ServiceName TYPE set(0) GRANULARITY 1,
    INDEX idx_metric  MetricName  TYPE set(0) GRANULARITY 1,
    INDEX idx_res_attr_key   mapKeys(ResourceAttributes)   TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = ReplacingMergeTree(LastSeen)
ORDER BY Fingerprint
SETTINGS index_granularity = 8192;`

const createDatapointsTableSQL = `
CREATE TABLE IF NOT EXISTS otel_metrics_datapoints (
    Fingerprint   UInt64 CODEC(Delta(8), ZSTD(1)),
    StartTimeUnix DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    TimeUnix      DateTime64(9) CODEC(DoubleDelta, ZSTD(1)),
    Value         Float64 CODEC(Gorilla, ZSTD(1)),
    Flags         UInt32 CODEC(ZSTD(1))
) ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (Fingerprint, TimeUnix)
TTL toDate(TimeUnix) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;`
