//go:build integration

package clickhouse_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"gdisw/metric-store/internal/store"
	chstore "gdisw/metric-store/internal/store/clickhouse"
)

// recentDatapointTime returns a TimeUnix in the last few days so TTL (30d) has not dropped the part.
func recentDatapointTime() time.Time {
	now := time.Now().UTC()
	return time.Date(now.Year(), now.Month(), now.Day(), 12, 0, 0, 0, time.UTC).Add(-2 * 24 * time.Hour)
}

func setup(t *testing.T) (*chstore.Store, func()) {
	t.Helper()
	ctx := context.Background()

	ctr, err := testcontainers.Run(ctx, "clickhouse/clickhouse-server:26.2",
		testcontainers.WithExposedPorts("9000/tcp"),
		testcontainers.WithEnv(map[string]string{
			"CLICKHOUSE_USER":     "default",
			"CLICKHOUSE_PASSWORD": "test",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("9000/tcp").WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}

	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("container host: %v", err)
	}
	mapped, err := ctr.MappedPort(ctx, "9000/tcp")
	if err != nil {
		t.Fatalf("mapped port: %v", err)
	}

	addr := fmt.Sprintf("%s:%s", host, mapped.Port())
	st, err := chstore.Open(ctx, chstore.Config{
		Addr:     addr,
		Database: "default",
		Username: "default",
		Password: "test",
	})
	if err != nil {
		_ = ctr.Terminate(ctx)
		t.Fatalf("open store: %v", err)
	}

	cleanup := func() {
		_ = st.Close()
		if err := ctr.Terminate(ctx); err != nil {
			t.Logf("terminate container: %v", err)
		}
	}
	return st, cleanup
}

func TestCreateTables_Integration(t *testing.T) {
	st, cleanup := setup(t)
	defer cleanup()

	ctx := context.Background()
	if err := st.CreateTables(ctx); err != nil {
		t.Fatalf("CreateTables: %v", err)
	}

	for _, name := range []string{"otel_metrics_metadata", "otel_metrics_datapoints"} {
		var n uint64
		err := st.Conn().QueryRow(ctx,
			"SELECT count() FROM system.tables WHERE database = 'default' AND name = $1", name,
		).Scan(&n)
		if err != nil {
			t.Fatalf("query system.tables: %v", err)
		}
		if n != 1 {
			t.Errorf("table %q: want count 1, got %d", name, n)
		}
	}
}

func TestStore_UpsertAndQueryDatapoint_Integration(t *testing.T) {
	st, cleanup := setup(t)
	defer cleanup()
	ctx := context.Background()
	if err := st.CreateTables(ctx); err != nil {
		t.Fatal(err)
	}

	fp := uint64(9001)
	seen := time.Now().UTC().Truncate(time.Second)
	t0 := seen.Add(1 * time.Second)
	t1 := seen.Add(2 * time.Second)

	if err := st.UpsertMetadata(ctx, []store.MetadataRow{{
		Fingerprint:       fp,
		MetricType:        store.MetricTypeGauge,
		ServiceName:       "svc",
		MetricName:        "cpu",
		FirstSeen:         seen,
		LastSeen:          seen,
		Attributes:        map[string]string{"k": "v"},
		ResourceSchemaUrl: "http://r",
		ScopeName:         "s",
		ScopeVersion:      "1",
	}}); err != nil {
		t.Fatalf("UpsertMetadata: %v", err)
	}
	if err := st.InsertDatapoints(ctx, []store.DatapointRow{{
		Fingerprint:   fp,
		StartTimeUnix: t0,
		TimeUnix:      t1,
		Value:         3.5,
		Flags:         0,
	}}); err != nil {
		t.Fatalf("InsertDatapoints: %v", err)
	}

	var gotName string
	var gotVal float64
	err := st.Conn().QueryRow(ctx, `
		SELECT m.MetricName, dp.Value
		FROM otel_metrics_datapoints AS dp
		INNER JOIN otel_metrics_metadata AS m ON m.Fingerprint = dp.Fingerprint
		WHERE m.MetricName = 'cpu'
	`).Scan(&gotName, &gotVal)
	if err != nil {
		t.Fatalf("join query: %v", err)
	}
	if gotName != "cpu" || gotVal != 3.5 {
		t.Fatalf("join result: name=%q val=%f", gotName, gotVal)
	}
}

func TestUpsertMetadataIdempotent_Integration(t *testing.T) {
	st, cleanup := setup(t)
	defer cleanup()
	ctx := context.Background()
	if err := st.CreateTables(ctx); err != nil {
		t.Fatal(err)
	}

	fp := uint64(42)
	tA := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	tB := time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC)
	row := func(last time.Time) store.MetadataRow {
		return store.MetadataRow{
			Fingerprint:            fp,
			MetricType:             store.MetricTypeSum,
			ServiceName:            "a",
			MetricName:             "m",
			FirstSeen:              tA,
			LastSeen:               last,
			AggregationTemporality: 1,
		}
	}
	for i := 0; i < 5; i++ {
		if err := st.UpsertMetadata(ctx, []store.MetadataRow{row(tB)}); err != nil {
			t.Fatalf("UpsertMetadata %d: %v", i, err)
		}
	}
	if err := st.Conn().Exec(ctx, "OPTIMIZE TABLE otel_metrics_metadata FINAL"); err != nil {
		t.Fatalf("OPTIMIZE: %v", err)
	}
	var n uint64
	if err := st.Conn().QueryRow(ctx, "SELECT count() FROM otel_metrics_metadata WHERE Fingerprint = $1", fp).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Fatalf("want 1 row after FINAL, got %d", n)
	}
	var last time.Time
	if err := st.Conn().QueryRow(ctx, "SELECT LastSeen FROM otel_metrics_metadata WHERE Fingerprint = $1", fp).Scan(&last); err != nil {
		t.Fatalf("LastSeen: %v", err)
	}
	if !last.Equal(tB) {
		t.Fatalf("LastSeen: got %v want %v", last, tB)
	}
}

func TestFingerprintChangesOnResourceMutation_Integration(t *testing.T) {
	st, cleanup := setup(t)
	defer cleanup()
	ctx := context.Background()
	if err := st.CreateTables(ctx); err != nil {
		t.Fatal(err)
	}

	seen := time.Unix(100, 0).UTC()
	rows := []store.MetadataRow{
		{
			Fingerprint:        1,
			MetricType:         store.MetricTypeGauge,
			ServiceName:        "s",
			MetricName:         "g",
			FirstSeen:          seen,
			LastSeen:           seen,
			ResourceAttributes: map[string]string{"env": "a"},
		},
		{
			Fingerprint:        2,
			MetricType:         store.MetricTypeGauge,
			ServiceName:        "s",
			MetricName:         "g",
			FirstSeen:          seen,
			LastSeen:           seen,
			ResourceAttributes: map[string]string{"env": "b"},
		},
	}
	if err := st.UpsertMetadata(ctx, rows); err != nil {
		t.Fatal(err)
	}
	var n uint64
	if err := st.Conn().QueryRow(ctx, "SELECT count() FROM otel_metrics_metadata").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("want 2 metadata rows, got %d", n)
	}
}

func TestGaugeAndSumCoexistInUnifiedTable_Integration(t *testing.T) {
	st, cleanup := setup(t)
	defer cleanup()
	ctx := context.Background()
	if err := st.CreateTables(ctx); err != nil {
		t.Fatal(err)
	}

	seen := time.Unix(200, 0).UTC()
	ts := recentDatapointTime()
	if err := st.UpsertMetadata(ctx, []store.MetadataRow{
		{Fingerprint: 10, MetricType: store.MetricTypeGauge, ServiceName: "s", MetricName: "x", FirstSeen: seen, LastSeen: seen},
		{Fingerprint: 20, MetricType: store.MetricTypeSum, ServiceName: "s", MetricName: "x", FirstSeen: seen, LastSeen: seen},
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.InsertDatapoints(ctx, []store.DatapointRow{
		{Fingerprint: 10, TimeUnix: ts, Value: 1, Flags: 0},
		{Fingerprint: 20, TimeUnix: ts, Value: 2, Flags: 0},
	}); err != nil {
		t.Fatal(err)
	}
	var c uint64
	if err := st.Conn().QueryRow(ctx, "SELECT count() FROM otel_metrics_datapoints WHERE Fingerprint IN (10, 20)").Scan(&c); err != nil {
		t.Fatal(err)
	}
	if c != 2 {
		t.Fatalf("datapoint rows: %d", c)
	}
}

func TestJoinFactLookupByType_Integration(t *testing.T) {
	st, cleanup := setup(t)
	defer cleanup()
	ctx := context.Background()
	if err := st.CreateTables(ctx); err != nil {
		t.Fatal(err)
	}

	seen := time.Unix(300, 0).UTC()
	t0 := recentDatapointTime()
	t1 := t0.Add(time.Second)
	if err := st.UpsertMetadata(ctx, []store.MetadataRow{
		{Fingerprint: 11, MetricType: store.MetricTypeGauge, ServiceName: "q", MetricName: "gonly", FirstSeen: seen, LastSeen: seen},
		{Fingerprint: 12, MetricType: store.MetricTypeSum, ServiceName: "q", MetricName: "gonly", FirstSeen: seen, LastSeen: seen},
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.InsertDatapoints(ctx, []store.DatapointRow{
		{Fingerprint: 11, TimeUnix: t0, Value: 1.1, Flags: 0},
		{Fingerprint: 12, TimeUnix: t1, Value: 2.2, Flags: 0},
	}); err != nil {
		t.Fatal(err)
	}

	var name string
	var val float64
	err := st.Conn().QueryRow(ctx, `
		SELECT m.MetricName, dp.Value
		FROM otel_metrics_datapoints AS dp
		INNER JOIN otel_metrics_metadata AS m ON m.Fingerprint = dp.Fingerprint
		WHERE m.MetricType = 'gauge'
		AND m.MetricName = 'gonly'
		AND dp.TimeUnix >= $1 AND dp.TimeUnix < $2
	`, t0.Add(-time.Second), t1.Add(time.Second),
	).Scan(&name, &val)
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	if name != "gonly" || val != 1.1 {
		t.Fatalf("row: name=%q val=%f", name, val)
	}
}

func TestPartitionPruning_EXPLAIN_Integration(t *testing.T) {
	st, cleanup := setup(t)
	defer cleanup()
	ctx := context.Background()
	if err := st.CreateTables(ctx); err != nil {
		t.Fatal(err)
	}

	// Two different partition days, still within TTL.
	base := time.Now().UTC()
	day1 := time.Date(base.Year(), base.Month(), base.Day(), 10, 0, 0, 0, time.UTC).AddDate(0, 0, -5)
	day2 := day1.Add(26 * time.Hour)
	seen := time.Unix(1, 0).UTC()
	if err := st.UpsertMetadata(ctx, []store.MetadataRow{
		{Fingerprint: 100, MetricType: store.MetricTypeGauge, ServiceName: "p", MetricName: "m", FirstSeen: seen, LastSeen: seen},
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.InsertDatapoints(ctx, []store.DatapointRow{
		{Fingerprint: 100, TimeUnix: day1, Value: 1, Flags: 0},
		{Fingerprint: 100, TimeUnix: day2, Value: 2, Flags: 0},
	}); err != nil {
		t.Fatal(err)
	}

	rows, err := st.Conn().Query(ctx, `
		EXPLAIN PLAN
		SELECT Fingerprint, TimeUnix, Value
		FROM otel_metrics_datapoints
		WHERE Fingerprint = 100
		AND TimeUnix >= $1 AND TimeUnix < $2
	`, day2, day2.Add(24*time.Hour),
	)
	if err != nil {
		t.Fatalf("EXPLAIN: %v", err)
	}
	var parts []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			t.Fatalf("scan: %v", err)
		}
		parts = append(parts, col)
	}
	_ = rows.Close()
	plan := strings.Join(parts, "\n")
	if !strings.Contains(plan, "MergeTree") {
		t.Fatalf("expected MergeTree in EXPLAIN, got:\n%s", plan)
	}
	// One row in the requested day, two total — proves query can target a partition.
	var cnt uint64
	if err := st.Conn().QueryRow(ctx, `
		SELECT count() FROM otel_metrics_datapoints
		WHERE Fingerprint = 100 AND TimeUnix >= $1 AND TimeUnix < $2
	`, day2, day2.Add(24*time.Hour),
	).Scan(&cnt); err != nil {
		t.Fatal(err)
	}
	if cnt != 1 {
		t.Fatalf("count in partition range: %d", cnt)
	}
}
