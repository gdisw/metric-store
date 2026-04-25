package main

import (
	"context"
	"log/slog"
	"time"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"

	"dash0.com/otlp-log-processor-backend/internal/otlpmap"
)

type dash0MetricsServiceServer struct {
	addr  string
	store MetricsStore

	colmetricspb.UnimplementedMetricsServiceServer
}

func newServer(addr string, store MetricsStore) colmetricspb.MetricsServiceServer {
	return &dash0MetricsServiceServer{addr: addr, store: store}
}

func (m *dash0MetricsServiceServer) Export(ctx context.Context, request *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	slog.DebugContext(ctx, "Received ExportMetricsServiceRequest")
	metricsReceivedCounter.Add(ctx, 1)

	if m.store != nil {
		batch := otlpmap.MapRequest(request, time.Now())
		gaugeRows, sumRows := mappedBatchToLegacy(batch)
		if len(gaugeRows) > 0 {
			if err := m.store.InsertGauge(ctx, gaugeRows); err != nil {
				return nil, err
			}
		}
		if len(sumRows) > 0 {
			if err := m.store.InsertSum(ctx, sumRows); err != nil {
				return nil, err
			}
		}
	}

	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}
