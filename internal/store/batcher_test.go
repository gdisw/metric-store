package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBatcher_FlushOnSizeThreshold(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mem := NewMemory()
	cfg := DefaultBatcherConfig()
	cfg.SizeFlushThreshold = 5
	cfg.FlushInterval = time.Hour // size path only

	b, err := NewBatcher(mem, cfg)
	if err != nil {
		t.Fatalf("NewBatcher: %v", err)
	}
	var rows []MetadataRow
	for i := range 5 {
		rows = append(rows, MetadataRow{Fingerprint: uint64(i + 1), ServiceName: "s"})
	}
	if err := b.Enqueue(ctx, rows, nil); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := b.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if mem.CountMetadata() != 5 {
		t.Fatalf("CountMetadata: got %d want 5", mem.CountMetadata())
	}
}

func TestBatcher_FlushOnInterval(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mem := NewMemory()
	cfg := DefaultBatcherConfig()
	cfg.SizeFlushThreshold = 100
	cfg.FlushInterval = 40 * time.Millisecond

	b, err := NewBatcher(mem, cfg)
	if err != nil {
		t.Fatalf("NewBatcher: %v", err)
	}
	if err := b.Enqueue(ctx, []MetadataRow{{Fingerprint: 99, ServiceName: "x"}}, nil); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	// Wait for the periodic tick to flush the non-empty batch.
	time.Sleep(120 * time.Millisecond)
	if err := b.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if mem.CountMetadata() != 1 {
		t.Fatalf("CountMetadata: got %d want 1", mem.CountMetadata())
	}
}

func TestBatcher_ShutdownDrainsChannels(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mem := NewMemory()
	cfg := DefaultBatcherConfig()
	cfg.SizeFlushThreshold = 1000
	cfg.FlushInterval = time.Hour
	cfg.MaxMetadataChannel = 8
	cfg.MaxDatapointChannel = 8

	b, err := NewBatcher(mem, cfg)
	if err != nil {
		t.Fatalf("NewBatcher: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := b.Enqueue(ctx, []MetadataRow{{Fingerprint: uint64(i) + 1}}, []DatapointRow{
			{Fingerprint: uint64(i) + 1, Value: float64(i)},
		}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	if err := b.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if mem.CountMetadata() != 3 {
		t.Fatalf("CountMetadata: got %d want 3", mem.CountMetadata())
	}
}

func TestBatcher_LRUSkipsSecondMetadataUpsert(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mem := NewMemory()
	cfg := DefaultBatcherConfig()
	cfg.SizeFlushThreshold = 10
	cfg.FlushInterval = 30 * time.Millisecond

	b, err := NewBatcher(mem, cfg)
	if err != nil {
		t.Fatalf("NewBatcher: %v", err)
	}
	fp := uint64(7)
	if err := b.Enqueue(ctx, []MetadataRow{{Fingerprint: fp, ServiceName: "svc", MetricName: "m"}}, nil); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	time.Sleep(80 * time.Millisecond)
	if err := b.Enqueue(ctx, []MetadataRow{{Fingerprint: fp, ServiceName: "svc", MetricName: "m"}}, nil); err != nil {
		t.Fatalf("Enqueue 2: %v", err)
	}
	if err := b.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if c := mem.MetadataUpsertCount(fp); c != 1 {
		t.Fatalf("MetadataUpsertCount: got %d want 1 (LRU should skip 2nd upsert)", c)
	}
}

func TestBatcher_BackpressureAndDropped(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mem := NewMemory(WithLatency(200 * time.Millisecond))
	cfg := DefaultBatcherConfig()
	// One received row triggers an immediate flush; the worker then blocks in Upsert.
	cfg.SizeFlushThreshold = 1
	cfg.FlushInterval = time.Hour
	cfg.MaxMetadataChannel = 1
	cfg.MaxDatapointChannel = 1

	b, err := NewBatcher(mem, cfg)
	if err != nil {
		t.Fatalf("NewBatcher: %v", err)
	}
	if err := b.Enqueue(ctx, []MetadataRow{{Fingerprint: 1}}, nil); err != nil {
		t.Fatalf("first Enqueue: %v", err)
	}
	// Wait until the worker has dequeued the row and is inside the slow Upsert (buffer must be free for the next line).
	time.Sleep(20 * time.Millisecond)
	// The one buffer slot is filled; the next send must be rejected.
	if err := b.Enqueue(ctx, []MetadataRow{{Fingerprint: 2}}, nil); err != nil {
		t.Fatalf("second Enqueue: %v", err)
	}
	if err := b.Enqueue(ctx, []MetadataRow{{Fingerprint: 3}}, nil); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("third Enqueue: got %v want ErrBackpressure", err)
	}
	if b.Dropped() != 1 {
		t.Fatalf("Dropped: got %d want 1", b.Dropped())
	}
	_ = b.Flush(context.Background())
}

func TestBatcher_RetryRecoversOnTransientStoreError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	errBoom := errors.New("transient")
	mem := NewMemory(WithFailingWriteCalls(1, errBoom))
	cfg := DefaultBatcherConfig()
	cfg.SizeFlushThreshold = 1
	cfg.FlushInterval = time.Hour
	cfg.MaxBatchRetries = 3

	b, err := NewBatcher(mem, cfg)
	if err != nil {
		t.Fatalf("NewBatcher: %v", err)
	}
	if err := b.Enqueue(ctx, []MetadataRow{{Fingerprint: 1, ServiceName: "a"}}, nil); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := b.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if mem.CountMetadata() != 1 {
		t.Fatalf("CountMetadata: got %d want 1", mem.CountMetadata())
	}
}

func TestBatcher_EnqueueAfterFlush(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mem := NewMemory()
	b, err := NewBatcher(mem, DefaultBatcherConfig())
	if err != nil {
		t.Fatalf("NewBatcher: %v", err)
	}
	_ = b.Flush(context.Background())
	if err := b.Enqueue(ctx, nil, nil); err == nil {
		t.Fatalf("Enqueue after Flush: want error")
	}
}

// Flush must not return while the worker still holds the store, even if the
// caller's flush context times out first (see grpcserver shutdown vs store.Close).
func TestBatcher_FlushWaitsForWorkerAfterTimeout(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mem := NewMemory(WithLatency(80 * time.Millisecond))
	cfg := DefaultBatcherConfig()
	cfg.SizeFlushThreshold = 100
	cfg.FlushInterval = time.Hour
	b, err := NewBatcher(mem, cfg)
	if err != nil {
		t.Fatalf("NewBatcher: %v", err)
	}
	if err := b.Enqueue(ctx, []MetadataRow{{Fingerprint: 1, ServiceName: "s"}}, nil); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	flushCtx, cancel := context.WithTimeout(context.Background(), 2*time.Millisecond)
	defer cancel()
	flushErr := b.Flush(flushCtx)
	if !errors.Is(flushErr, context.DeadlineExceeded) {
		t.Fatalf("Flush: want DeadlineExceeded, got %v", flushErr)
	}
	if mem.CountMetadata() != 1 {
		t.Fatalf("worker should finish store write after ctx timeout; CountMetadata=%d", mem.CountMetadata())
	}
}

func TestBatcher_DatapointOnlyBatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mem := NewMemory()
	cfg := DefaultBatcherConfig()
	cfg.SizeFlushThreshold = 1
	cfg.FlushInterval = time.Hour
	b, err := NewBatcher(mem, cfg)
	if err != nil {
		t.Fatalf("NewBatcher: %v", err)
	}
	if err := b.Enqueue(ctx, nil, []DatapointRow{{Fingerprint: 3, Value: 1.5}}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := b.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	dps := mem.DatapointsByFingerprint(3)
	if len(dps) != 1 || dps[0].Value != 1.5 {
		t.Fatalf("datapoints: got %#v", dps)
	}
}

// Regression: backpressure must not leave a prefix of metadata or datapoints
// queued (retries would duplicate datapoints in the store).
func TestBatcher_EnqueueAllOrNothingOnDatapointBackpressure(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mem := NewMemory(WithLatency(300 * time.Millisecond))
	cfg := DefaultBatcherConfig()
	cfg.SizeFlushThreshold = 1
	cfg.FlushInterval = time.Hour
	cfg.MaxMetadataChannel = 8
	cfg.MaxDatapointChannel = 2

	b, err := NewBatcher(mem, cfg)
	if err != nil {
		t.Fatalf("NewBatcher: %v", err)
	}
	// Worker blocks in Upsert; datapoints pile up in dpCh because the worker is not in its receive loop.
	if err := b.Enqueue(ctx, []MetadataRow{{Fingerprint: 1}}, nil); err != nil {
		t.Fatalf("Enqueue meta1: %v", err)
	}
	time.Sleep(25 * time.Millisecond)
	if err := b.Enqueue(ctx, nil, []DatapointRow{{Fingerprint: 1, Value: 1}}); err != nil {
		t.Fatalf("Enqueue dp1: %v", err)
	}
	if err := b.Enqueue(ctx, nil, []DatapointRow{{Fingerprint: 1, Value: 2}}); err != nil {
		t.Fatalf("Enqueue dp2: %v", err)
	}
	err = b.Enqueue(ctx,
		[]MetadataRow{{Fingerprint: 2, ServiceName: "s"}},
		[]DatapointRow{{Fingerprint: 2, Value: 3}, {Fingerprint: 2, Value: 4}},
	)
	if !errors.Is(err, ErrBackpressure) {
		t.Fatalf("combined Enqueue: got %v want ErrBackpressure", err)
	}
	if _, ok := mem.Metadata(2); ok {
		t.Fatal("metadata for fingerprint 2 must not appear when Enqueue fails")
	}
	if n := len(mem.DatapointsByFingerprint(2)); n != 0 {
		t.Fatalf("datapoints for fp 2: got %d want 0 before retry", n)
	}
	if got := b.Dropped(); got != 3 {
		t.Fatalf("Dropped: got %d want 3 (1 meta + 2 dps rejected as one batch)", got)
	}
	_ = b.Flush(context.Background())
}
