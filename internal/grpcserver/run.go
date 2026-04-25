package grpcserver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"dash0.com/otlp-log-processor-backend/internal/store"
)

// RunConfig controls the gRPC listener and batch writer.
type RunConfig struct {
	ListenAddr           string
	MaxRecvMsgSize       int
	Batcher              store.BatcherConfig
	ShutdownFlushTimeout time.Duration
}

func applyRunDefaults(cfg RunConfig) RunConfig {
	if cfg.MaxRecvMsgSize == 0 {
		cfg.MaxRecvMsgSize = 16 << 20
	}
	if cfg.ShutdownFlushTimeout == 0 {
		cfg.ShutdownFlushTimeout = 30 * time.Second
	}
	return cfg
}

// Run listens on cfg.ListenAddr, serves OTLP metrics, and blocks until ctx is cancelled.
// Shutdown order: GracefulStop → batcher flush → store close.
func Run(ctx context.Context, cfg RunConfig, st store.MetricsStore) error {
	cfg = applyRunDefaults(cfg)
	if cfg.ListenAddr == "" {
		_ = st.Close()
		return errors.New("grpcserver: empty ListenAddr")
	}

	bm, err := store.NewBatcherMetrics(otel.Meter(store.OTelScopeName))
	if err != nil {
		_ = st.Close()
		return fmt.Errorf("grpcserver: batcher metrics: %w", err)
	}
	cfg.Batcher.Metrics = bm

	batcher, err := store.NewBatcher(st, cfg.Batcher)
	if err != nil {
		_ = st.Close()
		return err
	}

	qreg, err := store.RegisterQueueDepthCallback(otel.Meter(store.OTelScopeName), batcher)
	if err != nil {
		_ = batcher.Flush(context.Background())
		_ = st.Close()
		return fmt.Errorf("grpcserver: queue depth callback: %w", err)
	}
	defer func() { _ = qreg.Unregister() }()

	lis, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		_ = batcher.Flush(context.Background())
		_ = st.Close()
		return err
	}

	exm, err := DefaultExportMetrics()
	if err != nil {
		_ = lis.Close()
		_ = batcher.Flush(context.Background())
		_ = st.Close()
		return fmt.Errorf("grpcserver: export metrics: %w", err)
	}

	srv := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.MaxRecvMsgSize(cfg.MaxRecvMsgSize),
		grpc.Creds(insecure.NewCredentials()),
	)
	colmetricspb.RegisterMetricsServiceServer(srv, NewMetricsService(batcher, exm))

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(lis) }()

	select {
	case <-ctx.Done():
		srv.GracefulStop()
	case err := <-errCh:
		flushCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownFlushTimeout)
		defer cancel()
		_ = batcher.Flush(flushCtx)
		_ = st.Close()
		return err
	}

	flushCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownFlushTimeout)
	defer cancel()
	flushErr := batcher.Flush(flushCtx)
	closeErr := st.Close()
	serveErr := <-errCh
	return errors.Join(flushErr, closeErr, serveErr)
}
