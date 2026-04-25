package grpcserver

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"gdisw/metric-store/internal/otlpmap"
	"gdisw/metric-store/internal/store"
)

var logOnce sync.Once
var logger *slog.Logger

// Logger returns the package logger (otelslog bridge), configured with
// [store.OTelScopeName] for trace/log correlation to the same meter.
//
// Call after the OpenTelemetry log SDK is installed (e.g. otelpipe.SetupOTelSDK)
// so the global log LoggerProvider is set; otherwise the bridge uses a no-op
// provider and container logs look empty.
func Logger() *slog.Logger {
	logOnce.Do(func() { logger = otelslog.NewLogger(store.OTelScopeName) })
	return logger
}

func logWithTrace(ctx context.Context) *slog.Logger {
	l := Logger()
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		l = l.With(
			"trace_id", sc.TraceID().String(),
			"span_id", sc.SpanID().String(),
		)
	}
	return l
}

// MetricsService implements OTLP metrics export using a [store.Batcher].
type MetricsService struct {
	batcher  *store.Batcher
	exporter *ExportMetrics
	colmetricspb.UnimplementedMetricsServiceServer
}

// NewMetricsService builds a gRPC handler that enqueues mapped rows into batcher.
// exporter may be nil in tests; production wiring should pass [DefaultExportMetrics].
func NewMetricsService(b *store.Batcher, exporter *ExportMetrics) *MetricsService {
	return &MetricsService{batcher: b, exporter: exporter}
}

// Export implements [colmetricspb.MetricsServiceServer].
func (s *MetricsService) Export(ctx context.Context, request *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	if s.exporter != nil {
		s.exporter.ExportReceived.Add(ctx, 1)
	}
	logWithTrace(ctx).DebugContext(ctx, "export metrics: received request")

	batch := otlpmap.MapRequest(request, time.Now())
	if len(batch.Metadata) == 0 && len(batch.Datapoints) == 0 {
		return &colmetricspb.ExportMetricsServiceResponse{}, nil
	}
	if err := s.batcher.Enqueue(ctx, batch.Metadata, batch.Datapoints); err != nil {
		if errors.Is(err, store.ErrBackpressure) {
			logWithTrace(ctx).WarnContext(ctx, "export metrics: backpressure, queue full", slog.String("err", err.Error()))
			return nil, status.Error(codes.ResourceExhausted, err.Error())
		}
		return nil, err
	}
	if s.exporter != nil {
		counts := datapointCountsByMetricType(batch)
		for typ, n := range counts {
			if n > 0 {
				s.exporter.DatapointsByType.Add(ctx, n, metric.WithAttributes(
					attribute.String("type", typ),
				))
			}
		}
	}
	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}

func datapointCountsByMetricType(batch otlpmap.MappedBatch) map[string]int64 {
	byFP := make(map[uint64]string, len(batch.Metadata))
	for _, m := range batch.Metadata {
		byFP[m.Fingerprint] = string(m.MetricType)
	}
	out := make(map[string]int64)
	for _, d := range batch.Datapoints {
		typ := byFP[d.Fingerprint]
		if typ == "" {
			typ = "unknown"
		}
		out[typ]++
	}
	return out
}
