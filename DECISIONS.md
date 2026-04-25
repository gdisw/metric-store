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

## Canonical layout (`cmd/`, `internal/`, mapper extraction)

I wanted to structure the project using the usual Go package layout (`cmd/server`, `internal/store`, `internal/otlpmap`, and so on). I’m not sure that was the “right” priority yet: it takes real time to move code and fix imports, and a flat `package main` would have been faster for the challenge. For anything that has to look like a production service, I think the layout is non-optional — clear boundaries, testable packages, and a place to hide implementation details. For a one-off or time-boxed exercise, it might be more of a nice-to-have.

**Context:** The metrics path used to map OTLP in the root module (`metrics_mapper.go`) and push wide rows into ClickHouse. The refactor plan called for a normalized model: `MappedBatch` with `[]MetadataRow` and `[]DatapointRow`, fingerprints computed per datapoint, gauge and sum sharing one scalar shape. That logic does not belong in `main` if we want the rest of the stack (batcher, gRPC, later CH adapter) to depend on stable types and tests without pulling in the server binary.

**Decision:** I moved mapping into `internal/otlpmap` (`MapRequest` + `MappedBatch`), wired fingerprints through `fingerprint.Identity` and `store` row types, and left a thin `mapped_legacy.go` in `main` to convert batches back to the existing wide `GaugeRow` / `SumRow` until the ClickHouse adapter matches the new schema. Tests for determinism, nil-safety, and type separation live next to the mapper.

**Trade-offs:** Clear separation and fast unit tests for mapping without Docker or CH; a temporary compatibility shim in `main` adds a bit of glue and means two mental models (normalized vs wide) until the last migration step. Up-front package moves slow velocity in the short run.

**Future optimization:** Once the CH store implements `MetricsStore` on the two-table design, delete `mapped_legacy.go` and have the server import only `internal/store` + `internal/otlpmap` + `internal/grpcserver` (or similar), with no wide-row types in the hot path. Optionally collapse or rename the root `package main` shims so `cmd/server` is the only entrypoint.