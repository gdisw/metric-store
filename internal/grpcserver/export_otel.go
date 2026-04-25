package grpcserver

import (
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"dash0.com/otlp-log-processor-backend/internal/store"
)

// ExportMetrics holds OTLP gRPC-level OpenTelemetry instruments for a single
// [Export] handler. A nil *ExportMetrics is safe: no points are recorded.
type ExportMetrics struct {
	ExportReceived   metric.Int64Counter
	DatapointsByType metric.Int64Counter
}

func DefaultExportMetrics() (*ExportMetrics, error) {
	m := otel.Meter(store.OTelScopeName)

	recv, err := m.Int64Counter("otlp.export.received",
		metric.WithDescription("gRPC ExportMetricsServiceRequest invocations (including empty)"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, fmt.Errorf("otlp.export.received: %w", err)
	}
	dp, err := m.Int64Counter("otlp.datapoints.processed",
		metric.WithDescription("Number of mapped scalar datapoints accepted for the store"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, fmt.Errorf("otlp.datapoints.processed: %w", err)
	}
	return &ExportMetrics{
		ExportReceived:   recv,
		DatapointsByType: dp,
	}, nil
}
