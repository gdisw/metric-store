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

## Interface-First Design and In-Memory Storage

**Context:**
I need to ship mapping, dedup, batching, and the gRPC path end-to-end, and the whole thing is supposed to land in ClickHouse eventually. I didn’t want to start by gluing all of that to the CH driver, Docker, and schema minutiae — that’s a separate rabbit hole.

**Decision:**
I’m leaning on a plain Go `MetricsStore` interface (`internal/store/store.go`) so I can wire and exercise the full pipeline first. I added a dumb in-memory impl (`internal/store/memory.go`) for fast unit tests, then I’ll dig into the ClickHouse-specific code when the vertical slice of “real” storage is the only thing left.