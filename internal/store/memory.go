package store

import (
	"context"
	"maps"
	"sync"
	"time"
)

type memoryStore struct {
	mu sync.RWMutex

	meta map[uint64]metadataEntry
	dps  map[uint64][]DatapointRow

	latency        time.Duration
	remainingFails int
	injectedErr    error
}

type metadataEntry struct {
	row     MetadataRow
	upserts int
}

type MemoryOption func(*memoryStore)

func NewMemory(opts ...MemoryOption) *memoryStore {
	m := &memoryStore{
		meta: make(map[uint64]metadataEntry),
		dps:  make(map[uint64][]DatapointRow),
	}
	for _, o := range opts {
		o(m)
	}
	return m
}

// WithLatency adds artificial delay to UpsertMetadata and InsertDatapoints
// (after optional injected errors) for backpressure and timing tests.
func WithLatency(d time.Duration) MemoryOption {
	return func(m *memoryStore) {
		m.latency = d
	}
}

// WithFailingWriteCalls makes the first n successful write paths
// (UpsertMetadata, InsertDatapoints — each call counts as one) return err.
// A single batch counts as one call. Retries in callers can use n > 1.
func WithFailingWriteCalls(n int, err error) MemoryOption {
	return func(m *memoryStore) {
		m.remainingFails = n
		m.injectedErr = err
	}
}

func (m *memoryStore) maybeDelay(ctx context.Context) error {
	if m.latency > 0 {
		t := time.NewTimer(m.latency)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
	return nil
}

func (m *memoryStore) maybeFailWrite() error {
	if m.remainingFails > 0 && m.injectedErr != nil {
		m.remainingFails--
		return m.injectedErr
	}
	return nil
}

// No-Op
func (m *memoryStore) CreateTables(_ context.Context) error {
	return nil
}

func (m *memoryStore) UpsertMetadata(ctx context.Context, rows []MetadataRow) error {
	if len(rows) == 0 {
		return nil
	}
	if err := m.maybeFailWrite(); err != nil {
		return err
	}
	if err := m.maybeDelay(ctx); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, row := range rows {
		ent, ok := m.meta[row.Fingerprint]
		if !ok {
			ent.row = cloneMetadataRow(row)
			ent.upserts = 1
		} else {
			r := cloneMetadataRow(row)
			r.FirstSeen = minTime(ent.row.FirstSeen, row.FirstSeen)
			r.LastSeen = maxTime(ent.row.LastSeen, row.LastSeen)
			ent.row = r
			ent.upserts++
		}
		m.meta[row.Fingerprint] = ent
	}
	return nil
}

func minTime(x, y time.Time) time.Time {
	if x.IsZero() {
		return y
	}
	if y.IsZero() {
		return x
	}
	if x.Before(y) {
		return x
	}
	return y
}

func maxTime(x, y time.Time) time.Time {
	if x.IsZero() {
		return y
	}
	if y.IsZero() {
		return x
	}
	if x.After(y) {
		return x
	}
	return y
}

func (m *memoryStore) InsertDatapoints(ctx context.Context, rows []DatapointRow) error {
	if len(rows) == 0 {
		return nil
	}
	if err := m.maybeFailWrite(); err != nil {
		return err
	}
	if err := m.maybeDelay(ctx); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, row := range rows {
		c := cloneDatapointRow(row)
		m.dps[row.Fingerprint] = append(m.dps[row.Fingerprint], c)
	}
	return nil
}

// No-Op
func (m *memoryStore) Close() error {
	return nil
}

func (m *memoryStore) CountMetadata() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.meta)
}

func (m *memoryStore) Metadata(fp uint64) (MetadataRow, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ent, ok := m.meta[fp]
	if !ok {
		return MetadataRow{}, false
	}
	return cloneMetadataRow(ent.row), true
}

func (m *memoryStore) MetadataUpsertCount(fp uint64) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ent, ok := m.meta[fp]
	if !ok {
		return 0
	}
	return ent.upserts
}

func (m *memoryStore) DatapointsByFingerprint(fp uint64) []DatapointRow {
	m.mu.RLock()
	defer m.mu.RUnlock()
	src := m.dps[fp]
	if len(src) == 0 {
		return nil
	}
	out := make([]DatapointRow, len(src))
	for i := range src {
		out[i] = cloneDatapointRow(src[i])
	}
	return out
}

func (m *memoryStore) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	clear(m.meta)
	m.dps = make(map[uint64][]DatapointRow)
}

var _ MetricsStore = (*memoryStore)(nil)

func cloneMetadataRow(r MetadataRow) MetadataRow {
	out := r
	if r.ResourceAttributes != nil {
		out.ResourceAttributes = maps.Clone(r.ResourceAttributes)
	}
	if r.ScopeAttributes != nil {
		out.ScopeAttributes = maps.Clone(r.ScopeAttributes)
	}
	if r.Attributes != nil {
		out.Attributes = maps.Clone(r.Attributes)
	}
	return out
}

func cloneDatapointRow(r DatapointRow) DatapointRow {
	return r
}
