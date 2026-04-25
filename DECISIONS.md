# Decisions Journal

This file documents the decisions made during the coding challenge.

## Deterministic Fingerprinting for Metric Series Identity

**Context:**
OTLP metrics are characterized by deeply nested metadata (Resource, Scope, and Datapoint attributes). To implement a normalized schema in ClickHouse, we need a stable 64-bit identifier (`SeriesID`) that uniquely represents a metric series. This ID must be calculated client-side to avoid expensive database round-trips or lookups during high-throughput ingestion.

**Decision:**
I implemented a custom fingerprinting package using the `xxhash/v2` algorithm. To ensure the hash is perfectly deterministic and collision-resistant, I adopted a manual serialization strategy:
1. **Canonicalization:** All map attributes are sorted by key before hashing to ensure that different iteration orders yield the same ID.
2. **Ambiguity Prevention:** Strings are serialized with a length prefix (varint) to prevent "sliding" collisions (e.g., distinguishing between `{"a","bc"}` and `{"ab","c"}`).
3. **Efficiency:** I avoided `encoding/json` or `fmt.Sprintf` to minimize memory allocations and CPU overhead in the hot path.

**Trade-offs:**
* **Pros:** Extremely high performance; deterministic across different SDKs; minimal storage footprint (8 bytes).
* **Cons:** Manual sorting of map keys adds a small CPU cost, which is necessary for consistency.

**Future Optimization:**
* To further reduce Garbage Collector (GC) pressure under extreme load, I identified the use of `sync.Pool` to reuse `xxhash.Digest` objects. This would move the implementation toward a "zero-allocation" model, which is ideal for a Staff-level production system.