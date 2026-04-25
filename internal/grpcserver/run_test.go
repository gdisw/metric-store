package grpcserver_test

import (
	"context"
	"sync/atomic"
	"testing"

	"gdisw/metric-store/internal/grpcserver"
	"gdisw/metric-store/internal/store"
)

// metricsStoreSpy counts Close calls for shutdown / leak regressions.
type metricsStoreSpy struct {
	closeCalls atomic.Int32
}

func (s *metricsStoreSpy) CreateTables(ctx context.Context) error { return nil }

func (s *metricsStoreSpy) UpsertMetadata(ctx context.Context, rows []store.MetadataRow) error {
	return nil
}

func (s *metricsStoreSpy) InsertDatapoints(ctx context.Context, rows []store.DatapointRow) error {
	return nil
}

func (s *metricsStoreSpy) Close() error {
	s.closeCalls.Add(1)
	return nil
}

func TestRun_ClosesStoreWhenListenAddrEmpty(t *testing.T) {
	t.Parallel()
	spy := &metricsStoreSpy{}
	ctx := context.Background()
	err := grpcserver.Run(ctx, grpcserver.RunConfig{ListenAddr: ""}, spy)
	if err == nil {
		t.Fatal("Run: want error for empty ListenAddr")
	}
	if spy.closeCalls.Load() != 1 {
		t.Fatalf("store Close calls: got %d want 1", spy.closeCalls.Load())
	}
}

func TestRun_ClosesStoreWhenNewBatcherFails(t *testing.T) {
	t.Parallel()
	spy := &metricsStoreSpy{}
	ctx := context.Background()
	err := grpcserver.Run(ctx, grpcserver.RunConfig{
		ListenAddr: "127.0.0.1:0",
		Batcher:    store.BatcherConfig{LRUMaxEntries: -1},
	}, spy)
	if err == nil {
		t.Fatal("Run: want error from NewBatcher")
	}
	if spy.closeCalls.Load() != 1 {
		t.Fatalf("store Close calls: got %d want 1", spy.closeCalls.Load())
	}
}

func TestRun_ClosesStoreWhenListenFails(t *testing.T) {
	t.Parallel()
	spy := &metricsStoreSpy{}
	ctx := context.Background()
	err := grpcserver.Run(ctx, grpcserver.RunConfig{
		ListenAddr: "localhost:notaport",
	}, spy)
	if err == nil {
		t.Fatal("Run: want error from net.Listen")
	}
	if spy.closeCalls.Load() != 1 {
		t.Fatalf("store Close calls: got %d want 1", spy.closeCalls.Load())
	}
}
