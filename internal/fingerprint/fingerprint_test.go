package fingerprint

import (
	"testing"
)

func sampleIdentity() Identity {
	return Identity{
		MetricType:        "gauge",
		ServiceName:       "checkout",
		MetricName:        "orders.total",
		MetricDescription: "Total orders",
		MetricUnit:        "1",
		ResourceAttributes: map[string]string{
			"service.version": "1.2.3",
			"deployment.env":  "prod",
		},
		ResourceSchemaUrl:      "https://example.com/resource",
		ScopeName:              "github.com/acme/pkg",
		ScopeVersion:           "v0.1.0",
		ScopeAttributes:        map[string]string{"lib": "otel"},
		ScopeSchemaUrl:         "https://example.com/scope",
		Attributes:             map[string]string{"color": "blue"},
		AggregationTemporality: 2,
		IsMonotonic:            false,
	}
}

func TestCompute_Deterministic(t *testing.T) {
	t.Parallel()
	id := sampleIdentity()
	a := Compute(id)
	b := Compute(id)
	if a != b {
		t.Fatalf("Compute(sample) = %d, second call = %d; want equal", a, b)
	}
}

func TestCompute_MapOrderingIndependent(t *testing.T) {
	t.Parallel()
	base := sampleIdentity()

	// Same logical identity; only composite literal insertion order for maps differs.
	a := Identity{
		MetricType:        base.MetricType,
		ServiceName:       base.ServiceName,
		MetricName:        base.MetricName,
		MetricDescription: base.MetricDescription,
		MetricUnit:        base.MetricUnit,
		ResourceAttributes: map[string]string{
			"z_last":  "1",
			"a_first": "2",
			"m_mid":   "3",
		},
		ResourceSchemaUrl: base.ResourceSchemaUrl,
		ScopeName:         base.ScopeName,
		ScopeVersion:      base.ScopeVersion,
		ScopeAttributes: map[string]string{
			"second": "y",
			"first":  "x",
		},
		ScopeSchemaUrl: base.ScopeSchemaUrl,
		Attributes: map[string]string{
			"gamma": "c",
			"alpha": "a",
			"beta":  "b",
		},
		AggregationTemporality: base.AggregationTemporality,
		IsMonotonic:            base.IsMonotonic,
	}

	b := Identity{
		MetricType:        a.MetricType,
		ServiceName:       a.ServiceName,
		MetricName:        a.MetricName,
		MetricDescription: a.MetricDescription,
		MetricUnit:        a.MetricUnit,
		ResourceAttributes: map[string]string{
			"m_mid":   "3",
			"z_last":  "1",
			"a_first": "2",
		},
		ResourceSchemaUrl: a.ResourceSchemaUrl,
		ScopeName:         a.ScopeName,
		ScopeVersion:      a.ScopeVersion,
		ScopeAttributes: map[string]string{
			"first":  "x",
			"second": "y",
		},
		ScopeSchemaUrl: a.ScopeSchemaUrl,
		Attributes: map[string]string{
			"alpha": "a",
			"beta":  "b",
			"gamma": "c",
		},
		AggregationTemporality: a.AggregationTemporality,
		IsMonotonic:            a.IsMonotonic,
	}

	c := Identity{
		MetricType:        a.MetricType,
		ServiceName:       a.ServiceName,
		MetricName:        a.MetricName,
		MetricDescription: a.MetricDescription,
		MetricUnit:        a.MetricUnit,
		ResourceAttributes: map[string]string{
			"a_first": "2",
			"m_mid":   "3",
			"z_last":  "1",
		},
		ResourceSchemaUrl: a.ResourceSchemaUrl,
		ScopeName:         a.ScopeName,
		ScopeVersion:      a.ScopeVersion,
		ScopeAttributes: map[string]string{
			"second": "y",
			"first":  "x",
		},
		ScopeSchemaUrl: a.ScopeSchemaUrl,
		Attributes: map[string]string{
			"beta":  "b",
			"gamma": "c",
			"alpha": "a",
		},
		AggregationTemporality: a.AggregationTemporality,
		IsMonotonic:            a.IsMonotonic,
	}

	if gotA, gotB, gotC := Compute(a), Compute(b), Compute(c); gotA != gotB || gotB != gotC {
		t.Fatalf("map literal order should not matter: %d, %d, %d", gotA, gotB, gotC)
	}
}

func TestCompute_DifferentIdentityDifferentHash(t *testing.T) {
	t.Parallel()
	base := sampleIdentity()
	cases := []struct {
		name string
		mut  func(Identity) Identity
	}{
		{
			name: "metric_type",
			mut: func(id Identity) Identity {
				id.MetricType = "sum"
				return id
			},
		},
		{
			name: "metric_name",
			mut: func(id Identity) Identity {
				id.MetricName = "orders.count"
				return id
			},
		},
		{
			name: "resource_attr_value",
			mut: func(id Identity) Identity {
				id.ResourceAttributes = map[string]string{
					"service.version": "1.2.4",
					"deployment.env":  "prod",
				}
				return id
			},
		},
		{
			name: "resource_attr_key",
			mut: func(id Identity) Identity {
				id.ResourceAttributes = map[string]string{
					"service.version": "1.2.3",
					"deployment.env":  "prod",
					"extra":           "x",
				}
				return id
			},
		},
		{
			name: "aggregation_temporality",
			mut: func(id Identity) Identity {
				id.AggregationTemporality = 1
				return id
			},
		},
		{
			name: "is_monotonic",
			mut: func(id Identity) Identity {
				id.IsMonotonic = true
				return id
			},
		},
	}

	baseHash := Compute(base)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			other := tc.mut(base)
			if h := Compute(other); h == baseHash {
				t.Fatalf("mutated identity hash %d equals base hash; expected different", h)
			}
		})
	}
}

func TestCompute_NilMapsEquivalentToEmpty(t *testing.T) {
	t.Parallel()
	withMaps := Identity{
		MetricType:             "gauge",
		ServiceName:            "s",
		MetricName:             "m",
		ResourceAttributes:     map[string]string{},
		ScopeAttributes:        map[string]string{},
		Attributes:             map[string]string{},
		AggregationTemporality: 0,
	}
	nilMaps := Identity{
		MetricType:             "gauge",
		ServiceName:            "s",
		MetricName:             "m",
		ResourceAttributes:     nil,
		ScopeAttributes:        nil,
		Attributes:             nil,
		AggregationTemporality: 0,
	}
	if a, b := Compute(withMaps), Compute(nilMaps); a != b {
		t.Fatalf("empty vs nil maps: %d vs %d", a, b)
	}
}
