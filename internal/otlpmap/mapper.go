package otlpmap

import (
	"fmt"
	"time"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"

	"gdisw/metric-store/internal/fingerprint"
	"gdisw/metric-store/internal/store"
)

type MappedBatch struct {
	Metadata   []store.MetadataRow
	Datapoints []store.DatapointRow
}

// MapRequest maps an ExportMetricsServiceRequest to store rows. Only Gauge and
// Sum scalar metrics are included; other instrument kinds are ignored.
// The same fingerprint is set on the corresponding MetadataRow and each DatapointRow.
// now is used for FirstSeen and LastSeen on every metadata row.
func MapRequest(req *colmetricspb.ExportMetricsServiceRequest, now time.Time) MappedBatch {
	if req == nil {
		return MappedBatch{}
	}
	var b MappedBatch
	seen := make(map[uint64]struct{})

	for _, rm := range req.GetResourceMetrics() {
		b.appendResourceMetrics(rm, now, &seen)
	}
	return b
}

func (b *MappedBatch) appendResourceMetrics(
	rm *metricspb.ResourceMetrics,
	now time.Time,
	seen *map[uint64]struct{},
) {
	if rm == nil {
		return
	}

	res := rm.GetResource()
	resSchemaURL := rm.GetSchemaUrl()
	var svcName string
	var resAttrs map[string]string
	if res != nil {
		svcName = serviceName(res)
		resAttrs = kvToMap(res.GetAttributes())
	}

	for _, sm := range rm.GetScopeMetrics() {
		if sm == nil {
			continue
		}
		scope := sm.GetScope()
		var (
			scopeName, scopeVersion, scopeSchemaURL string
			scopeAttrs                              map[string]string
			scopeDropped                            uint32
		)
		if scope != nil {
			scopeName = scope.GetName()
			scopeVersion = scope.GetVersion()
			scopeSchemaURL = sm.GetSchemaUrl()
			scopeAttrs = kvToMap(scope.GetAttributes())
			scopeDropped = scope.GetDroppedAttributesCount()
		} else {
			scopeSchemaURL = sm.GetSchemaUrl()
		}

		for _, metric := range sm.GetMetrics() {
			if metric == nil {
				continue
			}
			switch m := metric.GetData().(type) {
			case *metricspb.Metric_Gauge:
				if m.Gauge == nil {
					continue
				}
				for _, dp := range m.Gauge.GetDataPoints() {
					if dp == nil {
						continue
					}
					b.addScalarPoint(
						store.MetricTypeGauge,
						svcName, resAttrs, resSchemaURL,
						scopeName, scopeVersion, scopeSchemaURL, scopeAttrs, scopeDropped,
						metric,
						0, false,
						dp, now, seen,
					)
				}
			case *metricspb.Metric_Sum:
				if m.Sum == nil {
					continue
				}
				sum := m.Sum
				agg := int32(sum.GetAggregationTemporality())
				mon := sum.GetIsMonotonic()
				for _, dp := range sum.GetDataPoints() {
					if dp == nil {
						continue
					}
					b.addScalarPoint(
						store.MetricTypeSum,
						svcName, resAttrs, resSchemaURL,
						scopeName, scopeVersion, scopeSchemaURL, scopeAttrs, scopeDropped,
						metric,
						agg, mon,
						dp, now, seen,
					)
				}
			default:
			}
		}
	}
}

func (b *MappedBatch) addScalarPoint(
	metricType store.MetricType,
	svcName string,
	resAttrs map[string]string,
	resSchemaURL string,
	scopeName, scopeVersion, scopeSchemaURL string,
	scopeAttrs map[string]string,
	scopeDropped uint32,
	metric *metricspb.Metric,
	agg int32,
	isMonotonic bool,
	dp *metricspb.NumberDataPoint,
	now time.Time,
	seen *map[uint64]struct{},
) {
	dpAttrs := kvToMap(dp.GetAttributes())
	id := fingerprint.Identity{
		MetricType:             string(metricType),
		ServiceName:            svcName,
		MetricName:             metric.GetName(),
		MetricDescription:      metric.GetDescription(),
		MetricUnit:             metric.GetUnit(),
		ResourceAttributes:     resAttrs,
		ResourceSchemaUrl:      resSchemaURL,
		ScopeName:              scopeName,
		ScopeVersion:           scopeVersion,
		ScopeSchemaUrl:         scopeSchemaURL,
		ScopeAttributes:        scopeAttrs,
		Attributes:             dpAttrs,
		AggregationTemporality: agg,
		IsMonotonic:            isMonotonic,
	}
	fp := fingerprint.Compute(id)

	if _, ok := (*seen)[fp]; !ok {
		(*seen)[fp] = struct{}{}
		b.Metadata = append(b.Metadata, store.MetadataRow{
			Fingerprint:            fp,
			MetricType:             metricType,
			ServiceName:            svcName,
			MetricName:             metric.GetName(),
			MetricDescription:      metric.GetDescription(),
			MetricUnit:             metric.GetUnit(),
			ResourceAttributes:     resAttrs,
			ResourceSchemaUrl:      resSchemaURL,
			ScopeName:              scopeName,
			ScopeVersion:           scopeVersion,
			ScopeSchemaUrl:         scopeSchemaURL,
			ScopeAttributes:        scopeAttrs,
			ScopeDroppedAttrCount:  scopeDropped,
			Attributes:             dpAttrs,
			AggregationTemporality: agg,
			IsMonotonic:            isMonotonic,
			FirstSeen:              now,
			LastSeen:               now,
		})
	}

	b.Datapoints = append(b.Datapoints, store.DatapointRow{
		Fingerprint:   fp,
		StartTimeUnix: nanosToTime(dp.GetStartTimeUnixNano()),
		TimeUnix:      nanosToTime(dp.GetTimeUnixNano()),
		Value:         numberDataPointValue(dp),
		Flags:         dp.GetFlags(),
	})
}

// serviceName extracts the service.name from resource attributes, returning "" if not found.
func serviceName(resource *resourcepb.Resource) string {
	if resource == nil {
		return ""
	}
	for _, attr := range resource.GetAttributes() {
		if attr.GetKey() == "service.name" {
			return attr.GetValue().GetStringValue()
		}
	}
	return ""
}

// kvToMap converts a slice of OTLP KeyValue pairs to a Go map.
func kvToMap(attrs []*commonpb.KeyValue) map[string]string {
	if len(attrs) == 0 {
		return nil
	}
	m := make(map[string]string, len(attrs))
	for _, kv := range attrs {
		if kv == nil {
			continue
		}
		m[kv.GetKey()] = anyValueToString(kv.GetValue())
	}
	return m
}

// anyValueToString converts an OTLP AnyValue to its string representation.
func anyValueToString(v *commonpb.AnyValue) string {
	if v == nil {
		return ""
	}
	switch v.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return v.GetStringValue()
	case *commonpb.AnyValue_IntValue:
		return fmt.Sprintf("%d", v.GetIntValue())
	case *commonpb.AnyValue_DoubleValue:
		return fmt.Sprintf("%g", v.GetDoubleValue())
	case *commonpb.AnyValue_BoolValue:
		return fmt.Sprintf("%t", v.GetBoolValue())
	default:
		return fmt.Sprintf("%v", v)
	}
}

func nanosToTime(nanos uint64) time.Time {
	return time.Unix(0, int64(nanos))
}

// numberDataPointValue extracts the float64 value from a NumberDataPoint.
func numberDataPointValue(dp *metricspb.NumberDataPoint) float64 {
	switch v := dp.GetValue().(type) {
	case *metricspb.NumberDataPoint_AsDouble:
		return v.AsDouble
	case *metricspb.NumberDataPoint_AsInt:
		return float64(v.AsInt)
	default:
		return 0
	}
}
