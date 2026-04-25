# Decisions Journal

This file documents the decisions made during the coding challenge.

## Deterministic Fingerprinting for Metric Series Identity

### Context
OTLP metrics are characterized by deeply nested metadata (Resource, Scope, and Datapoint attributes). To implement a normalized schema in ClickHouse, we need a stable 64-bit identifier (`SeriesID`) that uniquely represents a metric series. This ID must be calculated client-side to avoid expensive database round-trips or lookups during high-throughput ingestion.

### Decision
I implemented a custom fingerprinting package using the `xxhash/v2` algorithm. To ensure the hash is perfectly deterministic and collision-resistant, I adopted a manual serialization strategy:
1. **Canonicalization:** All map attributes are sorted by key before hashing to ensure that different iteration orders yield the same ID.
2. **Ambiguity Prevention:** Strings are serialized with a length prefix (varint) to prevent "sliding" collisions (e.g., distinguishing between `{"a","bc"}` and `{"ab","c"}`).
3. **Efficiency:** I avoided `encoding/json` or `fmt.Sprintf` to minimize memory allocations and CPU overhead in the hot path.

### Trade-offs
* **Pros:** Extremely high performance; deterministic across different SDKs; minimal storage footprint (8 bytes).
* **Cons:** Manual sorting of map keys adds a small CPU cost, which is necessary for consistency.

### Future Optimization
* To further reduce Garbage Collector (GC) pressure under extreme load, I identified the use of `sync.Pool` to reuse `xxhash.Digest` objects. This would move the implementation toward a "zero-allocation" model, which is ideal for a Staff-level production system.

## Interface-First Design and In-Memory Storage

### Context
I need to ship mapping, dedup, batching, and the gRPC path end-to-end, and the whole thing is supposed to land in ClickHouse eventually. I didn’t want to start by gluing all of that to the CH driver, Docker, and schema minutiae — that’s a separate rabbit hole.

### Decision
I’m leaning on a plain Go `MetricsStore` interface (`internal/store/store.go`) so I can wire and exercise the full pipeline first. I added a dumb in-memory impl (`internal/store/memory.go`) for fast unit tests, then I’ll dig into the ClickHouse-specific code when the vertical slice of “real” storage is the only thing left.

## Canonical layout (`cmd/`, `internal/`, mapper extraction)

I wanted to structure the project using the usual Go package layout (`cmd/server`, `internal/store`, `internal/otlpmap`, and so on). I’m not sure that was the “right” priority yet: it takes real time to move code and fix imports, and a flat `package main` would have been faster for the challenge. For anything that has to look like a production service, I think the layout is non-optional — clear boundaries, testable packages, and a place to hide implementation details. For a one-off or time-boxed exercise, it might be more of a nice-to-have.

### Context
The metrics path used to map OTLP in the root module (`metrics_mapper.go`) and push wide rows into ClickHouse. The refactor plan called for a normalized model: `MappedBatch` with `[]MetadataRow` and `[]DatapointRow`, fingerprints computed per datapoint, gauge and sum sharing one scalar shape. That logic does not belong in `main` if we want the rest of the stack (batcher, gRPC, later CH adapter) to depend on stable types and tests without pulling in the server binary.

### Decision
I moved mapping into `internal/otlpmap` (`MapRequest` + `MappedBatch`), wired fingerprints through `fingerprint.Identity` and `store` row types, and left a thin `mapped_legacy.go` in `main` to convert batches back to the existing wide `GaugeRow` / `SumRow` until the ClickHouse adapter matches the new schema. Tests for determinism, nil-safety, and type separation live next to the mapper.

### Trade-offs
* **Pros:** Clear separation and fast unit tests for mapping without Docker or CH; a temporary compatibility shim in `main` adds a bit of glue and means two mental models (normalized vs wide) until the last migration step. Up-front package moves slow velocity in the short run.
* **Cons:** Up-front package moves slow velocity in the short run.

### Future Optimization
Once the CH store implements `MetricsStore` on the two-table design, delete `mapped_legacy.go` and have the server import only `internal/store` + `internal/otlpmap` + `internal/grpcserver` (or similar), with no wide-row types in the hot path. Optionally collapse or rename the root `package main` shims so `cmd/server` is the only entrypoint.

## Store batcher (LRU, queues, flush)

### Context
The ingest path should stay driver-agnostic until the ClickHouse adapter exists. We still need async batching, a way to avoid hammering the metadata table with the same fingerprint on every tick, backpressure when the writer falls behind, and a shutdown that actually drains to the store.

### Decision
I implemented `internal/store/batcher.go` against `MetricsStore` only: two bounded channels (metadata and datapoints), one worker, flush when `len(meta)+len(dps)` hits a size threshold *or* a periodic tick, and `Flush` to close down — drain the channels, flush what’s left, then exit the worker. Metadata “already pushed” is tracked with an LRU, but we only `Add` a fingerprint *after* a successful `UpsertMetadata`, so enqueue can skip the channel for cache hits. Writes use bounded retries and light exponential backoff; context cancel/Deadline are non-retriable. Order is always upsert metadata first, then datapoints; if insert fails, later attempts retry insert only, so the in-memory store test doesn’t double-append points. A full channel uses a non-blocking send and returns `ErrBackpressure` plus a dropped row counter for observability to hook into later.

### Trade-offs
* **Pros:** No ClickHouse in the import graph; the memory store is enough to test the full batching story; extra metadata upserts are harmless for ReplacingMergeTree / merge semantics in CH.
* **Cons:** The LRU is best-effort: races can still put the same new fingerprint in the channel twice before a flush, so you might upsert the same series twice (usually fine). Batches re-append to the in-memory buffer on a failed store write, so a permanently broken backend could grow memory (same class of problem as any unbounded retry queue without a cap).

### Future Optimization
* Emit the planned OTel metrics (`queue.dropped`, batch latency, etc.) on top of the existing hooks.
* Reuse growable slices across flushes (e.g. `buf = buf[:0]`) to cut allocations in the hot path; optional `sync.Pool` if profiling says it matters.

## gRPC server package, `cmd/server`, and shutdown order

### Context
The old `main.go` was a monolithic script: listen, serve, and hard-kill on exit. It also hardcoded the legacy wide-row schema. To ship a production-grade ingestion pipeline, we needed a real process lifecycle: stop accepting traffic, drain the queues, flush to disk, and disconnect cleanly.

### Decision
I split the entrypoint into `cmd/server/main.go` (process lifecycle, flags, OTel wiring) and `internal/grpcserver` (the OTLP handler).
Crucially, I wired a proper shutdown sequence tied to `SIGINT`/`SIGTERM`:
 1. `grpcServer.GracefulStop()` (stop taking new requests)
 2. `batcher.Flush(ctx)` (drain channels to the store)
 3. `store.Close()`

Because the final two-table ClickHouse schema isn't ready yet, I shoved the old wide-row logic into a temporary `internal/legacych` adapter that satisfies the new `MetricsStore` interface.

### Trade-offs
* **Pros:** The shutdown order is explicit and prod-ready. Backpressure maps correctly to `ResourceExhausted`.
* **Cons:** I debated whether splitting these packages (`cmd`, `grpcserver`, `legacych`) was premature. Moving code and fixing imports slowed down velocity. I could have shipped the signal handling + batcher inside a fat `main.go` and peeled it apart later. I did it now for the clean boundaries, but it definitely added mental overhead. Also, the temporary `legacych` adapter is a hack (caching metadata to rebuild wide rows), but it keeps tests green.

### Future Optimization
* Once the real ClickHouse adapter (with the two-table schema) lands, delete `legacych` entirely. If Ops requests it, we could add separate configurable timeouts for the gRPC drain vs. the batcher flush.
* Bind environment variables to the server flags (e.g. `LISTEN_ADDR`, `STORE`, ClickHouse creds) so config works the same in Docker/K8s without long command lines.
* Right now `main` calls `CreateTables` on boot — totally fine for “make it work” and `IF NOT EXISTS` is harmless enough for toy deploys. For actual production I’d **not** lean on that long-term: you want versioned migrations (or at least a dedicated migrate command / job / script), a stricter split of “who can DDL” vs “who can INSERT”, and the server assuming the schema is already there. `CREATE IF NOT EXISTS` doesn’t help you when you need `ALTER`s anyway.

## ClickHouse two-table adapter (`internal/store/clickhouse`)

### Context
The batcher, mapper, and gRPC stack already speak `store.MetricsStore` with `MetadataRow` / `DatapointRow`. The last step was researching and focus on ClickHouse. The plan was to stop expanding normalized rows back into the legacy wide `GaugeRow` / `SumRow` tables and write the new schema directly: a lookup table for series identity and a single fact table for scalar gauge/sum points.

### Decision
I added `internal/store/clickhouse` with DDL in `schema.go` and a native `Store` in `store.go` that implements `MetricsStore` end-to-end:
- **Metadata:** `otel_metrics_metadata` as `ReplacingMergeTree(LastSeen)` on `Fingerprint`, with `Enum8` for gauge vs sum and the usual resource/scope/attribute fields.
- **Fact:** `otel_metrics_datapoints` as a unified `MergeTree` with the same column layout for gauge and sum points (`Fingerprint`, `StartTimeUnix`, `TimeUnix`, `Value`, `Flags`), `PARTITION BY toDate(TimeUnix)`, `ORDER BY (Fingerprint, TimeUnix)`, and a 30-day TTL for bounded retention.
- **Advanced Codecs:** Applied TSDB-specific compression: `CODEC(DoubleDelta)` for timestamps and `CODEC(Gorilla)` for float values, drastically reducing the disk footprint compared to standard `LZ4`.
- **Inserts:** `PrepareBatch` + `Append` for both tables (no string `INSERT` loops). Connection settings set `async_insert = 1`. **`wait_for_async_insert`** defaults to **1** so a successful `Send()` is visible to the next `SELECT` in the same process (useful for tests and any read-after-write); `Config.SkipWaitForAsyncInsert` maps to `0` when you want maximum writer throughput and accept a short visibility lag.
- **Wiring:** `cmd/server` with `--store=clickhouse` opens this store. The old `internal/legacych` wide-row path remains in the tree for the moment (e.g. its integration tests) but is off the main server path.

### Trade-offs
* **Pros:** The ingestion path matches the data model: one metadata upsert per batch row and compact datapoint rows without repeating labels on every point. Partitions and `ORDER BY` line up with “no full table scan” for typical series+time queries.
* **Cons:** `CreateTables` still runs on server boot (same migration caveats as elsewhere). Replacing metadata dedup is asynchronous until `OPTIMIZE … FINAL` or merges — fine for the intended query patterns if you always join or filter on `Fingerprint` / time.

### Future Optimization
* Optional: a separate migration tool or versioned `ALTER`s instead of `CreateTables` in the server binary for production.

## Observability (OTel metrics and structured logging)

### Context
Ingest, batching, and the LRU cache have operational knobs (backpressure, flush latency, cache effectiveness) that are invisible without telemetry. The refactor plan called for a consistent signal model at the `store` abstraction (so the same metrics mean the same thing with `memory` or ClickHouse) and for logs to carry request correlation where traces exist.

### Decision
* **Shared scope name:** A single `store.OTelScopeName` string (`go.opentelemetry.io/otel` meter name) is used for the gRPC export path and the batcher so dashboards and the stdout metric exporter group instruments predictably. The default logger uses the `otelslog` bridge with the same name.
* **gRPC / mapper (`internal/grpcserver`):** `otlp.export.received` counts every `Export` RPC. `otlp.datapoints.processed` counts mapped scalar points **after** a successful `Enqueue` (so `ResourceExhausted` from backpressure does not inflate it), with attribute `type` = `gauge` / `sum` (or `unknown` if metadata is missing a fingerprint). Structured logs on the hot path add `trace_id` and `span_id` from `trace.SpanContextFromContext` for correlation; backpressure is logged at **warn** with the error.
* **Batcher (`internal/store`):** Optional `BatcherConfig.Metrics` wires `metadata.cache.hit` and `metadata.cache.miss` around the LRU peek in `Enqueue`. Successful `UpsertMetadata` / `InsertDatapoints` report `store.batch.inserted` (attribute `kind`) and `store.batch.latency` (seconds, by `kind`). Final failures after retries report `store.batch.failed` (with `kind` and a short `reason`). Channel saturation increments both the existing internal drop counter and `queue.dropped`. `queue.depth` is an `Int64ObservableGauge` with `RegisterQueueDepthCallback` on the meter, with attribute `queue` = `metadata` | `datapoints`.
* **Wiring:** `grpcserver.Run` builds `BatcherMetrics` for the given meter, sets `BatcherConfig.Metrics`, registers the queue-depth callback (unregistered on return), and creates `DefaultExportMetrics` for the gRPC service. The process must set the global `MeterProvider` (e.g. `otelpipe.SetupOTelSDK` in `cmd/server/main.go`) before `Run` for non-noop export.

### Trade-offs
* **Pros:** No ClickHouse or extra deps in the metric definitions beyond `otel/metric` and the existing SDK; the same instruments apply to the memory store in tests and to production.
* **Cons:** A large surface of instrument names and attributes; any rename is a breaking change for dashboards. Observable callbacks for queue depth are tied to the batcher’s lifetime. Datapoint “processed” is defined as accepted into the async queue, not yet persisted, which matches “past backpressure” but is not a strict end-to-end commit guarantee.

### Future Optimization
* Optional: export a OTLP metrics endpoint in addition to the stdout periodic reader, or pluggable exporters in `otelpipe` for different environments.
* Optional: add exemplars to histograms or link `store.batch` spans to the originating gRPC trace for deeper drill-down.
