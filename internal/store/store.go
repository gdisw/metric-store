package store

import (
	"context"
	"time"
)

type MetricType string

const (
	MetricTypeGauge MetricType = "gauge"
	MetricTypeSum   MetricType = "sum"
)

type MetadataRow struct {
	Fingerprint            uint64
	MetricType             MetricType
	ServiceName            string
	MetricName             string
	MetricDescription      string
	MetricUnit             string
	ResourceAttributes     map[string]string
	ResourceSchemaUrl      string
	ScopeName              string
	ScopeVersion           string
	ScopeSchemaUrl         string
	ScopeAttributes        map[string]string
	ScopeDroppedAttrCount  uint32
	Attributes             map[string]string
	AggregationTemporality int32
	IsMonotonic            bool
	FirstSeen              time.Time
	LastSeen               time.Time
}

type DatapointRow struct {
	Fingerprint   uint64
	StartTimeUnix time.Time
	TimeUnix      time.Time
	Value         float64
	Flags         uint32
}

type MetricsStore interface {
	CreateTables(ctx context.Context) error
	UpsertMetadata(ctx context.Context, rows []MetadataRow) error
	InsertDatapoints(ctx context.Context, rows []DatapointRow) error
	Close() error
}
