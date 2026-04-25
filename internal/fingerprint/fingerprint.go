package fingerprint

import (
	"encoding/binary"
	"io"
	"sort"

	"github.com/cespare/xxhash/v2"
)

// Identity captures the OTLP fields that define a metric series for fingerprinting.
// Map fields are hashed in key-sorted order so iteration order does not affect the result.
type Identity struct {
	MetricType             string
	ServiceName            string
	MetricName             string
	MetricDescription      string
	MetricUnit             string
	ResourceAttributes     map[string]string
	ResourceSchemaUrl      string
	ScopeName              string
	ScopeVersion           string
	ScopeAttributes        map[string]string
	ScopeSchemaUrl         string
	Attributes             map[string]string
	AggregationTemporality int32
	IsMonotonic            bool
}

// Compute returns a canonical xxhash64 over Identity. Two identities that differ in any
// field yield different hashes with overwhelming probability; equal identities always match.
func Compute(id Identity) uint64 {
	h := xxhash.New()
	writeString(h, id.MetricType)
	writeString(h, id.ServiceName)
	writeString(h, id.MetricName)
	writeString(h, id.MetricDescription)
	writeString(h, id.MetricUnit)
	writeStringMap(h, id.ResourceAttributes)
	writeString(h, id.ResourceSchemaUrl)
	writeString(h, id.ScopeName)
	writeString(h, id.ScopeVersion)
	writeStringMap(h, id.ScopeAttributes)
	writeString(h, id.ScopeSchemaUrl)
	writeStringMap(h, id.Attributes)
	writeInt32(h, id.AggregationTemporality)
	writeBool(h, id.IsMonotonic)
	return h.Sum64()
}

func writeString(w io.Writer, s string) {
	var lenBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lenBuf[:], uint64(len(s)))
	_, _ = w.Write(lenBuf[:n])
	_, _ = w.Write([]byte(s))
}

func writeStringMap(w io.Writer, m map[string]string) {
	if len(m) == 0 {
		return
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		writeString(w, k)
		writeString(w, m[k])
	}
}

func writeInt32(w io.Writer, v int32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(v))
	_, _ = w.Write(b[:])
}

func writeBool(w io.Writer, v bool) {
	if v {
		_, _ = w.Write([]byte{1})
		return
	}
	_, _ = w.Write([]byte{0})
}
