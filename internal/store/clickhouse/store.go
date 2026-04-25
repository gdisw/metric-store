package clickhouse

import (
	"context"
	"fmt"
	"maps"
	"time"

	chgo "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"gdisw/metric-store/internal/store"
)

type Config struct {
	Addr, Database, Username, Password string
	DialTimeout                        time.Duration
	SkipWaitForAsyncInsert             bool
}

type Store struct {
	conn driver.Conn
}

func Open(ctx context.Context, cfg Config) (*Store, error) {
	dt := cfg.DialTimeout
	if dt == 0 {
		dt = 5 * time.Second
	}
	conn, err := chgo.Open(&chgo.Options{
		Addr: []string{cfg.Addr},
		Auth: chgo.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Settings:    buildSettings(cfg),
		DialTimeout: dt,
	})
	if err != nil {
		return nil, fmt.Errorf("open clickhouse: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	return &Store{conn: conn}, nil
}

func (s *Store) Conn() driver.Conn { return s.conn }

func (s *Store) CreateTables(ctx context.Context) error {
	if err := s.conn.Exec(ctx, createMetadataTableSQL); err != nil {
		return fmt.Errorf("create metadata table: %w", err)
	}
	if err := s.conn.Exec(ctx, createDatapointsTableSQL); err != nil {
		return fmt.Errorf("create datapoints table: %w", err)
	}
	return nil
}

func (s *Store) UpsertMetadata(ctx context.Context, rows []store.MetadataRow) error {
	if len(rows) == 0 {
		return nil
	}
	b, err := s.conn.PrepareBatch(ctx, "INSERT INTO otel_metrics_metadata")
	if err != nil {
		return fmt.Errorf("prepare metadata batch: %w", err)
	}
	for _, r := range rows {
		mt, err := metricTypeEnum(r.MetricType)
		if err != nil {
			return err
		}
		if err := b.Append(
			r.Fingerprint,
			mt,
			r.ServiceName,
			r.MetricName,
			r.MetricDescription,
			r.MetricUnit,
			cloneMap(r.ResourceAttributes),
			r.ResourceSchemaUrl,
			r.ScopeName,
			r.ScopeVersion,
			cloneMap(r.ScopeAttributes),
			r.ScopeDroppedAttrCount,
			r.ScopeSchemaUrl,
			cloneMap(r.Attributes),
			r.AggregationTemporality,
			r.IsMonotonic,
			r.FirstSeen,
			r.LastSeen,
		); err != nil {
			return fmt.Errorf("append metadata: %w", err)
		}
	}
	if err := b.Send(); err != nil {
		return fmt.Errorf("send metadata batch: %w", err)
	}
	return nil
}

func (s *Store) InsertDatapoints(ctx context.Context, rows []store.DatapointRow) error {
	if len(rows) == 0 {
		return nil
	}
	b, err := s.conn.PrepareBatch(ctx, "INSERT INTO otel_metrics_datapoints")
	if err != nil {
		return fmt.Errorf("prepare datapoints batch: %w", err)
	}
	for _, r := range rows {
		if err := b.Append(
			r.Fingerprint,
			r.StartTimeUnix,
			r.TimeUnix,
			r.Value,
			r.Flags,
		); err != nil {
			return fmt.Errorf("append datapoint: %w", err)
		}
	}
	if err := b.Send(); err != nil {
		return fmt.Errorf("send datapoints batch: %w", err)
	}
	return nil
}

func (s *Store) Close() error {
	return s.conn.Close()
}

var _ store.MetricsStore = (*Store)(nil)

func buildSettings(cfg Config) chgo.Settings {
	w := 1
	if cfg.SkipWaitForAsyncInsert {
		w = 0
	}
	return chgo.Settings{
		"max_execution_time":    60,
		"async_insert":          1,
		"wait_for_async_insert": w,
	}
}

func metricTypeEnum(m store.MetricType) (string, error) {
	switch m {
	case store.MetricTypeGauge:
		return "gauge", nil
	case store.MetricTypeSum:
		return "sum", nil
	default:
		return "", fmt.Errorf("unknown metric type: %q", m)
	}
}

func cloneMap(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return maps.Clone(m)
}
