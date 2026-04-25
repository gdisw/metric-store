package legacych

import (
	"context"
	"sync"

	"dash0.com/otlp-log-processor-backend/internal/store"
)

// WideRowMetricsStore adapts the legacy wide-row ClickHouse [Store] to [store.MetricsStore]
// by retaining metadata in memory and expanding datapoints into gauge/sum inserts.
type WideRowMetricsStore struct {
	inner *Store
	mu    sync.Mutex
	meta  map[uint64]store.MetadataRow
}

// NewWideRowMetricsStore wraps ch for use with the batcher and OTLP mapper.
func NewWideRowMetricsStore(ch *Store) *WideRowMetricsStore {
	return &WideRowMetricsStore{
		inner: ch,
		meta:  make(map[uint64]store.MetadataRow),
	}
}

func (w *WideRowMetricsStore) CreateTables(ctx context.Context) error {
	return w.inner.CreateTables(ctx)
}

func (w *WideRowMetricsStore) UpsertMetadata(ctx context.Context, rows []store.MetadataRow) error {
	_ = ctx
	if len(rows) == 0 {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, r := range rows {
		w.meta[r.Fingerprint] = r
	}
	return nil
}

func (w *WideRowMetricsStore) InsertDatapoints(ctx context.Context, rows []store.DatapointRow) error {
	if len(rows) == 0 {
		return nil
	}
	w.mu.Lock()
	var gauges []GaugeRow
	var sums []SumRow
	for _, dp := range rows {
		m, ok := w.meta[dp.Fingerprint]
		if !ok {
			continue
		}
		g := GaugeRow{
			ResourceAttributes:    m.ResourceAttributes,
			ResourceSchemaUrl:     m.ResourceSchemaUrl,
			ScopeName:             m.ScopeName,
			ScopeVersion:          m.ScopeVersion,
			ScopeAttributes:       m.ScopeAttributes,
			ScopeDroppedAttrCount: m.ScopeDroppedAttrCount,
			ScopeSchemaUrl:        m.ScopeSchemaUrl,
			ServiceName:           m.ServiceName,
			MetricName:            m.MetricName,
			MetricDescription:     m.MetricDescription,
			MetricUnit:            m.MetricUnit,
			Attributes:            m.Attributes,
			StartTimeUnix:         dp.StartTimeUnix,
			TimeUnix:              dp.TimeUnix,
			Value:                 dp.Value,
			Flags:                 dp.Flags,
		}
		switch m.MetricType {
		case store.MetricTypeGauge:
			gauges = append(gauges, g)
		case store.MetricTypeSum:
			sums = append(sums, SumRow{
				GaugeRow:               g,
				AggregationTemporality: m.AggregationTemporality,
				IsMonotonic:            m.IsMonotonic,
			})
		}
	}
	w.mu.Unlock()

	if len(gauges) > 0 {
		if err := w.inner.InsertGauge(ctx, gauges); err != nil {
			return err
		}
	}
	if len(sums) > 0 {
		if err := w.inner.InsertSum(ctx, sums); err != nil {
			return err
		}
	}
	return nil
}

func (w *WideRowMetricsStore) Close() error {
	return w.inner.Close()
}

var _ store.MetricsStore = (*WideRowMetricsStore)(nil)
