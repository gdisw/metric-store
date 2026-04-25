package otlpmap

import (
	"testing"
	"time"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"

	"dash0.com/otlp-log-processor-backend/internal/store"
)

func TestMapRequest_Deterministic(t *testing.T) {
	t.Parallel()
	now := time.Unix(1700000000, 123456789)
	req := makeExportRequest(t)
	a := MapRequest(req, now)
	b := MapRequest(req, now)
	if len(a.Metadata) != len(b.Metadata) || len(a.Datapoints) != len(b.Datapoints) {
		t.Fatalf("MapRequest not deterministic: metadata %d vs %d, dps %d vs %d",
			len(a.Metadata), len(b.Metadata), len(a.Datapoints), len(b.Datapoints))
	}
	for i := range a.Metadata {
		if a.Metadata[i].Fingerprint != b.Metadata[i].Fingerprint {
			t.Fatalf("metadata[%d] fingerprint: %d vs %d", i, a.Metadata[i].Fingerprint, b.Metadata[i].Fingerprint)
		}
	}
	for i := range a.Datapoints {
		if a.Datapoints[i] != b.Datapoints[i] {
			t.Fatalf("datapoints[%d] differ: %+v vs %+v", i, a.Datapoints[i], b.Datapoints[i])
		}
	}
}

func TestMapRequest_IdenticalIdentityTwoRequests_SameFingerprint(t *testing.T) {
	t.Parallel()
	now := time.Unix(1700000000, 0)
	req := makeExportRequest(t)
	a := MapRequest(req, now)
	b := MapRequest(req, now)
	if len(a.Metadata) != len(b.Metadata) {
		t.Fatalf("metadata len %d vs %d", len(a.Metadata), len(b.Metadata))
	}
	for i := range a.Metadata {
		if a.Metadata[i].Fingerprint != b.Metadata[i].Fingerprint {
			t.Fatalf("request %d: fp %d vs %d", i, a.Metadata[i].Fingerprint, b.Metadata[i].Fingerprint)
		}
	}
}

func TestMapRequest_DifferentMetricType_DifferentFingerprint(t *testing.T) {
	t.Parallel()
	now := time.Unix(1700000000, 0)
	req := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "api"}}},
					},
				},
				SchemaUrl: "res",
				ScopeMetrics: []*metricspb.ScopeMetrics{
					{
						SchemaUrl: "scope",
						Scope:     &commonpb.InstrumentationScope{Name: "lib", Version: "1"},
						Metrics: []*metricspb.Metric{
							{
								Name:        "dupe",
								Description: "d",
								Unit:        "1",
								Data: &metricspb.Metric_Gauge{
									Gauge: &metricspb.Gauge{
										DataPoints: []*metricspb.NumberDataPoint{
											{TimeUnixNano: 100, Value: &metricspb.NumberDataPoint_AsInt{AsInt: 1}},
										},
									},
								},
							},
							{
								Name:        "dupe",
								Description: "d",
								Unit:        "1",
								Data: &metricspb.Metric_Sum{
									Sum: &metricspb.Sum{
										AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
										IsMonotonic:            true,
										DataPoints: []*metricspb.NumberDataPoint{
											{TimeUnixNano: 200, Value: &metricspb.NumberDataPoint_AsInt{AsInt: 2}},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	batch := MapRequest(req, now)
	if len(batch.Metadata) != 2 {
		t.Fatalf("expected 2 metadata rows (gauge + sum), got %d", len(batch.Metadata))
	}
	if batch.Metadata[0].Fingerprint == batch.Metadata[1].Fingerprint {
		t.Fatalf("gauge and sum for same name must not collide, fp=%d", batch.Metadata[0].Fingerprint)
	}
	var sawGauge, sawSum bool
	for _, r := range batch.Metadata {
		switch r.MetricType {
		case store.MetricTypeGauge:
			sawGauge = true
		case store.MetricTypeSum:
			sawSum = true
		}
	}
	if !sawGauge || !sawSum {
		t.Fatalf("expected one gauge and one sum in metadata, saw gauge=%v sum=%v", sawGauge, sawSum)
	}
}

func TestMapRequest_NilRequestEmpty(t *testing.T) {
	t.Parallel()
	b := MapRequest(nil, time.Time{})
	if len(b.Metadata) != 0 || len(b.Datapoints) != 0 {
		t.Fatalf("nil request: want empty, got %d metadata, %d datapoints", len(b.Metadata), len(b.Datapoints))
	}
}

func TestMapRequest_NilAndPartial_NoPanic(t *testing.T) {
	t.Parallel()
	now := time.Unix(1, 0)
	for _, name := range []string{
		"empty",
		"nil resource",
		"nil scope",
		"nil metric",
		"nil data point",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var req *colmetricspb.ExportMetricsServiceRequest
			switch name {
			case "empty":
				req = &colmetricspb.ExportMetricsServiceRequest{}
			case "nil resource":
				req = &colmetricspb.ExportMetricsServiceRequest{
					ResourceMetrics: []*metricspb.ResourceMetrics{
						{
							ScopeMetrics: []*metricspb.ScopeMetrics{
								{
									Metrics: []*metricspb.Metric{
										{
											Name: "m",
											Data: &metricspb.Metric_Gauge{
												Gauge: &metricspb.Gauge{
													DataPoints: []*metricspb.NumberDataPoint{},
												},
											},
										},
									},
								},
							},
						},
					},
				}
			case "nil scope":
				req = &colmetricspb.ExportMetricsServiceRequest{
					ResourceMetrics: []*metricspb.ResourceMetrics{
						{
							Resource: &resourcepb.Resource{},
							ScopeMetrics: []*metricspb.ScopeMetrics{
								{Metrics: []*metricspb.Metric{
									{Name: "m", Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
										DataPoints: []*metricspb.NumberDataPoint{{Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 1}}},
									}}},
								}},
							},
						},
					},
				}
			case "nil metric":
				req = &colmetricspb.ExportMetricsServiceRequest{
					ResourceMetrics: []*metricspb.ResourceMetrics{
						{ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{nil}}}},
					},
				}
			case "nil data point":
				req = &colmetricspb.ExportMetricsServiceRequest{
					ResourceMetrics: []*metricspb.ResourceMetrics{
						{ScopeMetrics: []*metricspb.ScopeMetrics{{
							Metrics: []*metricspb.Metric{
								{Name: "m", Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
									DataPoints: []*metricspb.NumberDataPoint{nil},
								}}},
							},
						}}},
					},
				}
			}
			b := MapRequest(req, now)
			_ = b.Metadata
			_ = b.Datapoints
		})
	}
}

func TestMapRequest_DatapointSharesMetadataFingerprint(t *testing.T) {
	t.Parallel()
	now := time.Unix(1, 0)
	req := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{
			{
				Resource:  &resourcepb.Resource{},
				SchemaUrl: "r",
				ScopeMetrics: []*metricspb.ScopeMetrics{
					{SchemaUrl: "s", Scope: &commonpb.InstrumentationScope{}, Metrics: []*metricspb.Metric{
						{Name: "x", Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
							DataPoints: []*metricspb.NumberDataPoint{
								{TimeUnixNano: 1, Value: &metricspb.NumberDataPoint_AsInt{AsInt: 1}},
								{TimeUnixNano: 2, Value: &metricspb.NumberDataPoint_AsInt{AsInt: 2}},
							},
						}}},
					}},
				},
			},
		},
	}
	b := MapRequest(req, now)
	if len(b.Metadata) != 1 {
		t.Fatalf("two identical label sets → one metadata, got %d", len(b.Metadata))
	}
	if len(b.Datapoints) != 2 {
		t.Fatalf("want 2 datapoints, got %d", len(b.Datapoints))
	}
	if b.Metadata[0].Fingerprint != b.Datapoints[0].Fingerprint || b.Metadata[0].Fingerprint != b.Datapoints[1].Fingerprint {
		t.Fatalf("metadata and datapoint fingerprints should match, meta=%d dp0=%d dp1=%d",
			b.Metadata[0].Fingerprint, b.Datapoints[0].Fingerprint, b.Datapoints[1].Fingerprint)
	}
}

// --- test helpers

func makeExportRequest(t *testing.T) *colmetricspb.ExportMetricsServiceRequest {
	t.Helper()
	return &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test"}}},
					},
				},
				SchemaUrl: "https://example/res",
				ScopeMetrics: []*metricspb.ScopeMetrics{
					{
						SchemaUrl: "https://example/scope",
						Scope:     &commonpb.InstrumentationScope{Name: "scope", Version: "v1"},
						Metrics: []*metricspb.Metric{
							{
								Name:    "g",
								Unit:    "1",
								Data:    &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
									DataPoints: []*metricspb.NumberDataPoint{
										{TimeUnixNano: 100, Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 3.14}},
									},
								}},
							},
						},
					},
				},
			},
		},
	}
}

