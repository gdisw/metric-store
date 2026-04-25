//go:build integration

package legacych_test

import (
	"context"
	"fmt"
	"log"
	"net"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"dash0.com/otlp-log-processor-backend/internal/grpcserver"
	"dash0.com/otlp-log-processor-backend/internal/legacych"
	"dash0.com/otlp-log-processor-backend/internal/otlpmap"
	"dash0.com/otlp-log-processor-backend/internal/store"
)

func testGRPCMetricsReceivedCounter(t *testing.T) metric.Int64Counter {
	t.Helper()
	m := noop.NewMeterProvider().Meter(grpcserver.MeterName)
	c, err := m.Int64Counter(grpcserver.MetricsReceivedInstrument,
		metric.WithDescription("The number of metrics received by otlp-metrics-processor-backend"),
		metric.WithUnit("{metric}"))
	if err != nil {
		t.Fatalf("Int64Counter: %v", err)
	}
	return c
}

func setupClickHouse(t *testing.T) (*legacych.Store, func()) {
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
		t.Fatalf("starting clickhouse container: %v", err)
	}

	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("getting container host: %v", err)
	}
	mappedPort, err := ctr.MappedPort(ctx, "9000/tcp")
	if err != nil {
		t.Fatalf("getting mapped port: %v", err)
	}

	addr := fmt.Sprintf("%s:%s", host, mappedPort.Port())
	st, err := legacych.NewStore(ctx, addr, "default", "default", "test")
	if err != nil {
		t.Fatalf("creating clickhouse metrics store: %v", err)
	}

	cleanup := func() {
		_ = st.Close()
		if err := ctr.Terminate(ctx); err != nil {
			t.Logf("terminating clickhouse container: %v", err)
		}
	}

	return st, cleanup
}

func mappedBatchToWideRows(batch otlpmap.MappedBatch) (gauges []legacych.GaugeRow, sums []legacych.SumRow) {
	if len(batch.Datapoints) == 0 {
		return nil, nil
	}
	meta := make(map[uint64]store.MetadataRow, len(batch.Metadata))
	for _, m := range batch.Metadata {
		meta[m.Fingerprint] = m
	}
	for _, dp := range batch.Datapoints {
		m, ok := meta[dp.Fingerprint]
		if !ok {
			continue
		}
		g := legacych.GaugeRow{
			ResourceAttributes:    m.ResourceAttributes,
			ResourceSchemaUrl:     m.ResourceSchemaUrl,
			ScopeName:             m.ScopeName,
			ScopeVersion:          m.ScopeVersion,
			ScopeAttributes:       m.ScopeAttributes,
			ScopeDroppedAttrCount: m.ScopeDroppedAttrCount,
			ScopeSchemaUrl:        m.ScopeSchemaUrl,
			ServiceName:           m.ServiceName,
			MetricName:            m.MetricName,
			MetricDescription:     m.MetricDescription,
			MetricUnit:            m.MetricUnit,
			Attributes:            m.Attributes,
			StartTimeUnix:         dp.StartTimeUnix,
			TimeUnix:              dp.TimeUnix,
			Value:                 dp.Value,
			Flags:                 dp.Flags,
		}
		switch m.MetricType {
		case store.MetricTypeGauge:
			gauges = append(gauges, g)
		case store.MetricTypeSum:
			sums = append(sums, legacych.SumRow{
				GaugeRow:               g,
				AggregationTemporality: m.AggregationTemporality,
				IsMonotonic:            m.IsMonotonic,
			})
		}
	}
	return gauges, sums
}

func TestCreateTables(t *testing.T) {
	st, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := st.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	expectedTables := []string{
		"otel_metrics_gauge",
		"otel_metrics_sum",
		"otel_metrics_histogram",
		"otel_metrics_exponential_histogram",
		"otel_metrics_summary",
	}

	for _, table := range expectedTables {
		var count uint64
		err := st.Conn.QueryRow(ctx,
			"SELECT count() FROM system.tables WHERE database = 'default' AND name = $1", table,
		).Scan(&count)
		if err != nil {
			t.Fatalf("querying system.tables for %s: %v", table, err)
		}
		if count != 1 {
			t.Errorf("expected table %s to exist, got count=%d", table, count)
		}
	}
}

func TestInsertGauge(t *testing.T) {
	st, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := st.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	now := uint64(time.Now().UnixNano())
	startTime := now - uint64(time.Minute)
	resourceMetrics := []*metricspb.ResourceMetrics{
		{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test-service"}}},
					{Key: "host.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test-host"}}},
				},
			},
			SchemaUrl: "https://opentelemetry.io/schemas/1.4.0",
			ScopeMetrics: []*metricspb.ScopeMetrics{
				{
					Scope: &commonpb.InstrumentationScope{
						Name:    "test-scope",
						Version: "1.0.0",
					},
					Metrics: []*metricspb.Metric{
						{
							Name:        "cpu.utilization",
							Description: "CPU utilization percentage",
							Unit:        "%",
							Data: &metricspb.Metric_Gauge{
								Gauge: &metricspb.Gauge{
									DataPoints: []*metricspb.NumberDataPoint{
										{
											Attributes:        []*commonpb.KeyValue{{Key: "cpu", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "0"}}}},
											StartTimeUnixNano: startTime,
											TimeUnixNano:      now,
											Value:             &metricspb.NumberDataPoint_AsDouble{AsDouble: 42.5},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	batch := otlpmap.MapRequest(&colmetricspb.ExportMetricsServiceRequest{ResourceMetrics: resourceMetrics}, time.Now())
	rows, _ := mappedBatchToWideRows(batch)
	if err := st.InsertGauge(ctx, rows); err != nil {
		t.Fatalf("inserting gauge rows: %v", err)
	}

	var (
		serviceName string
		metricName  string
		value       float64
	)
	err := st.Conn.QueryRow(ctx,
		"SELECT ServiceName, MetricName, Value FROM otel_metrics_gauge WHERE MetricName = 'cpu.utilization'",
	).Scan(&serviceName, &metricName, &value)
	if err != nil {
		t.Fatalf("querying gauge: %v", err)
	}

	if serviceName != "test-service" {
		t.Errorf("expected ServiceName=test-service, got %s", serviceName)
	}
	if metricName != "cpu.utilization" {
		t.Errorf("expected MetricName=cpu.utilization, got %s", metricName)
	}
	if value != 42.5 {
		t.Errorf("expected Value=42.5, got %f", value)
	}
}

func TestInsertSum(t *testing.T) {
	st, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := st.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	now := uint64(time.Now().UnixNano())
	startTime := now - uint64(time.Minute)
	resourceMetrics := []*metricspb.ResourceMetrics{
		{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test-service"}}},
					{Key: "host.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test-host"}}},
				},
			},
			SchemaUrl: "https://opentelemetry.io/schemas/1.4.0",
			ScopeMetrics: []*metricspb.ScopeMetrics{
				{
					Scope: &commonpb.InstrumentationScope{
						Name:    "test-scope",
						Version: "1.0.0",
					},
					Metrics: []*metricspb.Metric{
						{
							Name:        "http.requests.total",
							Description: "Total HTTP requests",
							Unit:        "{request}",
							Data: &metricspb.Metric_Sum{
								Sum: &metricspb.Sum{
									AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
									IsMonotonic:            true,
									DataPoints: []*metricspb.NumberDataPoint{
										{
											Attributes: []*commonpb.KeyValue{
												{Key: "method", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "GET"}}},
												{Key: "status", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "200"}}},
											},
											StartTimeUnixNano: startTime,
											TimeUnixNano:      now,
											Value:             &metricspb.NumberDataPoint_AsDouble{AsDouble: 1234},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	batch := otlpmap.MapRequest(&colmetricspb.ExportMetricsServiceRequest{ResourceMetrics: resourceMetrics}, time.Now())
	_, rows := mappedBatchToWideRows(batch)
	if err := st.InsertSum(ctx, rows); err != nil {
		t.Fatalf("inserting sum rows: %v", err)
	}

	var (
		serviceName            string
		metricName             string
		value                  float64
		aggregationTemporality int32
		isMonotonic            bool
	)
	err := st.Conn.QueryRow(ctx,
		"SELECT ServiceName, MetricName, Value, AggregationTemporality, IsMonotonic FROM otel_metrics_sum WHERE MetricName = 'http.requests.total'",
	).Scan(&serviceName, &metricName, &value, &aggregationTemporality, &isMonotonic)
	if err != nil {
		t.Fatalf("querying sum: %v", err)
	}

	if serviceName != "test-service" {
		t.Errorf("expected ServiceName=test-service, got %s", serviceName)
	}
	if metricName != "http.requests.total" {
		t.Errorf("expected MetricName=http.requests.total, got %s", metricName)
	}
	if value != 1234 {
		t.Errorf("expected Value=1234, got %f", value)
	}
	if aggregationTemporality != 2 {
		t.Errorf("expected AggregationTemporality=2, got %d", aggregationTemporality)
	}
	if !isMonotonic {
		t.Errorf("expected IsMonotonic=true, got false")
	}
}

func TestGRPCToClickHouse(t *testing.T) {
	chStore, cleanup := setupClickHouse(t)
	defer cleanup()

	ctx := context.Background()
	if err := chStore.CreateTables(ctx); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	adapter := legacych.NewWideRowMetricsStore(chStore)
	batcher, err := store.NewBatcher(adapter, store.DefaultBatcherConfig())
	if err != nil {
		t.Fatalf("NewBatcher: %v", err)
	}
	t.Cleanup(func() {
		_ = batcher.Flush(context.Background())
	})

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	colmetricspb.RegisterMetricsServiceServer(grpcServer, grpcserver.NewMetricsService(batcher, testGRPCMetricsReceivedCounter(t)))
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("error serving server: %v", err)
		}
	}()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("connecting to grpc server: %v", err)
	}
	defer conn.Close()

	client := colmetricspb.NewMetricsServiceClient(conn)

	now := uint64(time.Now().UnixNano())
	_, err = client.Export(ctx, &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "e2e-service"}}},
					},
				},
				ScopeMetrics: []*metricspb.ScopeMetrics{
					{
						Scope: &commonpb.InstrumentationScope{Name: "e2e-scope"},
						Metrics: []*metricspb.Metric{
							{
								Name: "e2e.gauge",
								Data: &metricspb.Metric_Gauge{
									Gauge: &metricspb.Gauge{
										DataPoints: []*metricspb.NumberDataPoint{
											{
												TimeUnixNano: now,
												Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: 99.9},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("exporting metrics via grpc: %v", err)
	}

	if err := batcher.Flush(ctx); err != nil {
		t.Fatalf("flush batcher: %v", err)
	}

	var (
		svcName    string
		metricName string
		value      float64
	)
	err = chStore.Conn.QueryRow(ctx,
		"SELECT ServiceName, MetricName, Value FROM otel_metrics_gauge WHERE MetricName = 'e2e.gauge'",
	).Scan(&svcName, &metricName, &value)
	if err != nil {
		t.Fatalf("querying clickhouse: %v", err)
	}
	if svcName != "e2e-service" {
		t.Errorf("expected ServiceName=e2e-service, got %s", svcName)
	}
	if value != 99.9 {
		t.Errorf("expected Value=99.9, got %f", value)
	}
}
