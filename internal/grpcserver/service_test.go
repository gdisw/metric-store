package grpcserver_test

import (
	"context"
	"log"
	"net"
	"testing"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"dash0.com/otlp-log-processor-backend/internal/grpcserver"
	"dash0.com/otlp-log-processor-backend/internal/otlpmap"
	"dash0.com/otlp-log-processor-backend/internal/store"
)

func testExportMetrics(t *testing.T) *grpcserver.ExportMetrics {
	t.Helper()
	m := noop.NewMeterProvider().Meter(store.OTelScopeName)
	recv, err := m.Int64Counter("otlp.export.received",
		metric.WithDescription("gRPC ExportMetricsServiceRequest invocations (including empty)"),
		metric.WithUnit("1"),
	)
	if err != nil {
		t.Fatalf("otlp.export.received: %v", err)
	}
	dp, err := m.Int64Counter("otlp.datapoints.processed",
		metric.WithDescription("Number of mapped scalar datapoints accepted for the store"),
		metric.WithUnit("1"),
	)
	if err != nil {
		t.Fatalf("otlp.datapoints.processed: %v", err)
	}
	return &grpcserver.ExportMetrics{ExportReceived: recv, DatapointsByType: dp}
}

func TestMetricsService_Export(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	mem := store.NewMemory()
	b, err := store.NewBatcher(mem, store.DefaultBatcherConfig())
	if err != nil {
		t.Fatalf("NewBatcher: %v", err)
	}
	t.Cleanup(func() {
		_ = b.Flush(context.Background())
	})

	buffer := 101024 * 1024
	lis := bufconn.Listen(buffer)
	srv := grpc.NewServer()
	colmetricspb.RegisterMetricsServiceServer(srv, grpcserver.NewMetricsService(b, testExportMetrics(t)))
	go func() {
		if err := srv.Serve(lis); err != nil {
			log.Printf("bufconn serve: %v", err)
		}
	}()
	t.Cleanup(func() {
		_ = lis.Close()
		srv.Stop()
	})

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	client := colmetricspb.NewMetricsServiceClient(conn)

	t.Run("empty_request", func(t *testing.T) {
		out, err := client.Export(ctx, &colmetricspb.ExportMetricsServiceRequest{
			ResourceMetrics: []*metricspb.ResourceMetrics{
				{
					ScopeMetrics: []*metricspb.ScopeMetrics{},
					SchemaUrl:    "dash0.com/otlp-metrics-processor-backend",
				},
			},
		})
		if err != nil {
			t.Fatalf("Export: %v", err)
		}
		if out.GetPartialSuccess().GetRejectedDataPoints() != 0 ||
			out.GetPartialSuccess().GetErrorMessage() != "" {
			t.Fatalf("unexpected partial success: %+v", out.GetPartialSuccess())
		}
	})

	t.Run("gauge_roundtrip_memory", func(t *testing.T) {
		mem.Reset()
		now := uint64(time.Now().UnixNano())
		req := &colmetricspb.ExportMetricsServiceRequest{
			ResourceMetrics: []*metricspb.ResourceMetrics{
				{
					Resource: &resourcepb.Resource{
						Attributes: []*commonpb.KeyValue{
							{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "svc"}}},
						},
					},
					ScopeMetrics: []*metricspb.ScopeMetrics{
						{
							Metrics: []*metricspb.Metric{
								{
									Name: "cpu",
									Data: &metricspb.Metric_Gauge{
										Gauge: &metricspb.Gauge{
											DataPoints: []*metricspb.NumberDataPoint{
												{TimeUnixNano: now, Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 3.5}},
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
		batch := otlpmap.MapRequest(req, time.Now())
		if len(batch.Datapoints) != 1 {
			t.Fatalf("expected 1 datapoint in mapped batch, got %d", len(batch.Datapoints))
		}
		fp := batch.Datapoints[0].Fingerprint

		_, err := client.Export(ctx, req)
		if err != nil {
			t.Fatalf("Export: %v", err)
		}
		if err := b.Flush(ctx); err != nil {
			t.Fatalf("Flush: %v", err)
		}
		if mem.CountMetadata() != 1 {
			t.Fatalf("metadata rows: got %d want 1", mem.CountMetadata())
		}
		dps := mem.DatapointsByFingerprint(fp)
		if len(dps) != 1 {
			t.Fatalf("datapoints for fp: got %d want 1", len(dps))
		}
		if dps[0].Value != 3.5 {
			t.Fatalf("Value: got %v want 3.5", dps[0].Value)
		}
	})
}
