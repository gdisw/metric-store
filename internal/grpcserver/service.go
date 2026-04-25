package grpcserver

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"dash0.com/otlp-log-processor-backend/internal/otlpmap"
	"dash0.com/otlp-log-processor-backend/internal/store"
)

const (
	MeterName                 = "dash0.com/otlp-log-processor-backend"
	MetricsReceivedInstrument = "com.dash0.homeexercise.metrics.received"
)

var logger = otelslog.NewLogger(MeterName)

// DefaultMetricsReceivedCounter builds the standard counter from the global [otel.MeterProvider]
// (configure OTel before calling; used by [Run] and production wiring).
func DefaultMetricsReceivedCounter() (metric.Int64Counter, error) {
	m := otel.Meter(MeterName)
	return m.Int64Counter(MetricsReceivedInstrument,
		metric.WithDescription("The number of metrics received by otlp-metrics-processor-backend"),
		metric.WithUnit("{metric}"))
}

// Logger returns the package logger (otelslog bridge).
func Logger() *slog.Logger {
	return logger
}

// MetricsService implements OTLP metrics export using a [store.Batcher].
type MetricsService struct {
	batcher         *store.Batcher
	metricsReceived metric.Int64Counter
	colmetricspb.UnimplementedMetricsServiceServer
}

// NewMetricsService builds a gRPC handler that enqueues mapped rows into batcher.
// metricsReceived is invoked on every Export (use [DefaultMetricsReceivedCounter] in production, or a noop/test meter in tests).
func NewMetricsService(b *store.Batcher, metricsReceived metric.Int64Counter) *MetricsService {
	return &MetricsService{batcher: b, metricsReceived: metricsReceived}
}

// Export implements [colmetricspb.MetricsServiceServer].
func (s *MetricsService) Export(ctx context.Context, request *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	slog.DebugContext(ctx, "Received ExportMetricsServiceRequest")
	s.metricsReceived.Add(ctx, 1)

	batch := otlpmap.MapRequest(request, time.Now())
	if len(batch.Metadata) == 0 && len(batch.Datapoints) == 0 {
		return &colmetricspb.ExportMetricsServiceResponse{}, nil
	}
	if err := s.batcher.Enqueue(ctx, batch.Metadata, batch.Datapoints); err != nil {
		if errors.Is(err, store.ErrBackpressure) {
			return nil, status.Error(codes.ResourceExhausted, err.Error())
		}
		return nil, err
	}
	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}
