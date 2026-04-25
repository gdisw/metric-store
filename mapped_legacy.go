package main

import (
	"dash0.com/otlp-log-processor-backend/internal/otlpmap"
	"dash0.com/otlp-log-processor-backend/internal/store"
)

// mappedBatchToLegacy converts a mapped batch into the wide-row shapes used by the
// current ClickHouse metrics store. Every datapoint is matched to its series via Fingerprint.
func mappedBatchToLegacy(b otlpmap.MappedBatch) (gauges []GaugeRow, sums []SumRow) {
	if len(b.Datapoints) == 0 {
		return nil, nil
	}
	meta := make(map[uint64]store.MetadataRow, len(b.Metadata))
	for _, m := range b.Metadata {
		meta[m.Fingerprint] = m
	}
	for _, dp := range b.Datapoints {
		m, ok := meta[dp.Fingerprint]
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
	return gauges, sums
}
