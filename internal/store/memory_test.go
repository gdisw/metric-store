package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestMemory_UpsertAndQuery(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	m := NewMemory()
	fp := uint64(42)
	t0 := time.Unix(1, 0).UTC()
	t1 := time.Unix(2, 0).UTC()

	if err := m.UpsertMetadata(ctx, []MetadataRow{{
		Fingerprint: fp, ServiceName: "svc", MetricName: "c",
		FirstSeen: t1, LastSeen: t1,
	}}); err != nil {
		t.Fatalf("UpsertMetadata: %v", err)
	}
	if m.CountMetadata() != 1 {
		t.Fatalf("CountMetadata: got %d want 1", m.CountMetadata())
	}
	r, ok := m.Metadata(fp)
	if !ok {
		t.Fatalf("Metadata: missing row")
	}
	if r.ServiceName != "svc" || r.FirstSeen != t1 || r.LastSeen != t1 {
		t.Fatalf("Metadata: got %+v", r)
	}
	if m.MetadataUpsertCount(fp) != 1 {
		t.Fatalf("MetadataUpsertCount: got %d want 1", m.MetadataUpsertCount(fp))
	}

	if err := m.UpsertMetadata(ctx, []MetadataRow{{
		Fingerprint: fp, ServiceName: "svc", MetricName: "c",
		FirstSeen: t0, LastSeen: t0,
	}}); err != nil {
		t.Fatalf("UpsertMetadata 2: %v", err)
	}
	r, _ = m.Metadata(fp)
	if r.FirstSeen != t0 {
		t.Fatalf("FirstSeen: got %v want %v", r.FirstSeen, t0)
	}
	if r.LastSeen != t1 {
		t.Fatalf("LastSeen: got %v want %v", r.LastSeen, t1)
	}
	if m.MetadataUpsertCount(fp) != 2 {
		t.Fatalf("MetadataUpsertCount: got %d want 2", m.MetadataUpsertCount(fp))
	}
}

func TestMemory_DatapointsByFingerprint(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	m := NewMemory()
	fp := uint64(7)
	s := time.Unix(10, 0).UTC()
	tm := time.Unix(20, 0).UTC()
	if err := m.InsertDatapoints(ctx, []DatapointRow{
		{Fingerprint: fp, StartTimeUnix: s, TimeUnix: tm, Value: 1, Flags: 0},
		{Fingerprint: fp, StartTimeUnix: s, TimeUnix: tm, Value: 2, Flags: 1},
	}); err != nil {
		t.Fatalf("InsertDatapoints: %v", err)
	}
	dps := m.DatapointsByFingerprint(fp)
	if len(dps) != 2 {
		t.Fatalf("len: got %d want 2", len(dps))
	}
	if dps[0].Value != 1 || dps[1].Value != 2 {
		t.Fatalf("values: got %#v", dps)
	}
	// External mutation must not affect store.
	dps[0].Value = 99
	dps2 := m.DatapointsByFingerprint(fp)
	if dps2[0].Value != 1 {
		t.Fatalf("isolation: got %v want 1", dps2[0].Value)
	}
}

func TestMemory_CreateTablesAndClose(t *testing.T) {
	t.Parallel()
	m := NewMemory()
	if err := m.CreateTables(context.Background()); err != nil {
		t.Fatalf("CreateTables: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestMemory_Reset(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	m := NewMemory()
	if err := m.UpsertMetadata(ctx, []MetadataRow{{Fingerprint: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := m.InsertDatapoints(ctx, []DatapointRow{{Fingerprint: 1}}); err != nil {
		t.Fatal(err)
	}
	m.Reset()
	if m.CountMetadata() != 0 {
		t.Fatalf("CountMetadata after Reset: %d", m.CountMetadata())
	}
	if dps := m.DatapointsByFingerprint(1); len(dps) != 0 {
		t.Fatalf("Datapoints after Reset: len %d", len(dps))
	}
}

func TestMemory_WithFailingWriteCalls(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	errBoom := errors.New("boom")
	m := NewMemory(WithFailingWriteCalls(1, errBoom))
	if err := m.UpsertMetadata(ctx, []MetadataRow{{Fingerprint: 1}}); !errors.Is(err, errBoom) {
		t.Fatalf("first Upsert: got %v want %v", err, errBoom)
	}
	if err := m.UpsertMetadata(ctx, []MetadataRow{{Fingerprint: 1}}); err != nil {
		t.Fatalf("second Upsert: %v", err)
	}
}

func TestMemory_WithLatency_CancelledContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	m := NewMemory(WithLatency(time.Hour))
	if err := m.UpsertMetadata(ctx, []MetadataRow{{Fingerprint: 1}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Upsert: got %v want Canceled", err)
	}
}

func TestMemory_ConcurrentWriters(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	m := NewMemory()
	const n = 64
	var (
		wg sync.WaitGroup
		mu sync.Mutex
		ferr error
	)
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			fp := uint64(i)
			if err := m.UpsertMetadata(ctx, []MetadataRow{{Fingerprint: fp}}); err != nil {
				mu.Lock()
				if ferr == nil {
					ferr = fmt.Errorf("UpsertMetadata: %w", err)
				}
				mu.Unlock()
				return
			}
			if err := m.InsertDatapoints(ctx, []DatapointRow{{Fingerprint: fp, Value: float64(i)}}); err != nil {
				mu.Lock()
				if ferr == nil {
					ferr = fmt.Errorf("InsertDatapoints: %w", err)
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if ferr != nil {
		t.Fatal(ferr)
	}
	if m.CountMetadata() != n {
		t.Fatalf("CountMetadata: got %d want %d", m.CountMetadata(), n)
	}
}
