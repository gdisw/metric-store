# Improvements for production-level deployment

This document tracks the work that would still be needed before treating this OTLP metric storage service as a real production system. The current code is a working vertical slice (gRPC ingest → mapper → batcher → ClickHouse / memory), and this list captures the gaps to close — grouped by concern, with a rough sense of priority.

> Legend: **P0** — required before any prod traffic. **P1** — required before scaling out / multi-tenant. **P2** — quality-of-life / cost / nice-to-have.

---

## 1. Security and transport hardening

- **P0 — TLS / mTLS on the gRPC listener.** `internal/grpcserver/run.go` uses `grpc.Creds(insecure.NewCredentials())`. Production must terminate TLS on the listener (or behind a sidecar) and ideally require client certs for collector→backend fan-in. Wire `credentials.NewTLS(...)` from a configurable cert/key path or a secret mount.
- **P0 — AuthN / AuthZ.** Today any client that can reach the port can ingest. Add at minimum a bearer-token / API-key interceptor (`grpc.UnaryInterceptor`/`StreamInterceptor`) and reject unauthenticated requests; ideally mTLS + token, with per-tenant identity for authz and rate limiting.
- **P0 — Don’t pass ClickHouse credentials via `--clickhouse.password`.** `cmd/server/main.go` exposes the password as a CLI flag — visible in `ps`, shells, container inspect, etc. Read it from a file path (`--clickhouse.password-file`) or env var, and integrate with the platform secret manager (K8s Secret, Vault, AWS SM…).
- **P1 — Rate limiting / quotas.** Add per-peer or per-tenant rate limiting at the gRPC layer (token bucket interceptor). Pair with the existing backpressure (`ErrBackpressure` → `ResourceExhausted`) so noisy tenants can’t starve good ones.
- **P1 — PII / sensitive attribute scrubbing.** `internal/otlpmap/mapper.go` ingests every resource/scope/attribute key as-is. Add a configurable allow/deny list and a hashing/redaction step before persistence.
- **P2 — Request size + concurrency caps per peer.** `MaxRecvMsgSize` is global; consider per-connection limits and `grpc.MaxConcurrentStreams`.

---

## 2. Reliability and durability

- **P0 — In-process queues are lossy on crash / OOM.** `store.Batcher` keeps everything in two unbuffered-on-disk channels; a SIGKILL or OOM drops every queued row. Options:
  - Add a disk-backed WAL (e.g., a small append-only file or `badger`/`bbolt`) that is replayed on restart.
  - Or, push the durability boundary back to the client by making the OTLP `Export` call only return success once the batch is **flushed** (synchronous mode toggle) rather than “queued”. The current trade-off is documented but should be a knob.
- **P0 — Bounded retries can give up silently.** `writeBatchesWithRetry` exhausts after `MaxBatchRetries` and re-appends to the in-memory accumulators in `doFlush`. With a permanently broken backend this grows memory until OOM. Add an upper bound on accumulator size with a deadletter sink (file / S3 / dropped-with-counter) and an alert on `store.batch.failed`.
- **P0 — `CreateTables` on boot is not a migration story.** `cmd/server/main.go` runs DDL on every start. It cannot do `ALTER`s, can race in multi-replica deployments, and gives every server `CREATE TABLE` privileges. Replace with:
  - A separate `cmd/migrate` binary (or a `Job` in K8s) that runs versioned migrations (`golang-migrate`, `goose`, `atlas`).
  - The runtime user gets only `INSERT`/`SELECT`; the migrator user gets DDL.
- **P1 — ClickHouse replication / HA.** `schema.go` uses plain `MergeTree` / `ReplacingMergeTree`. For prod you almost certainly want `Replicated*MergeTree` on a Keeper-backed cluster, with shard/replica-aware connection settings (`Cluster`, `Hosts`) in `chstore.Config`.
- **P1 — Connection resilience.** `clickhouse.Open` only `Ping`s once and never reconnects. Add `MaxOpenConns`, `MaxIdleConns`, `ConnMaxLifetime`, healthcheck loop, and a circuit breaker so the batcher backs off (instead of looping retries) when ClickHouse is down.
- **P1 — `enqueueMu` serializes every `Export`.** The batcher takes a single mutex on the hot enqueue path (`internal/store/batcher.go`). Under high RPS this becomes a bottleneck. Options: shard queues by fingerprint, or replace the “check capacity then send” pattern with non-blocking sends + bounded retry.
- **P2 — Deduplication on retry.** The batcher’s “upsert metadata first, then datapoints, retry datapoints only” logic is correct for the in-memory store but does not protect against double-inserts if a ClickHouse `Send()` fails after partial server-side ingest. Consider idempotency keys per batch or accept the duplication for `MergeTree`.

---

## 3. Observability

- **P0 — Replace stdout exporters with OTLP.** `internal/otelpipe/setup.go` wires `stdouttrace`, `stdoutmetric`, `stdoutlog` (all `WithPrettyPrint`). In production this floods stdout, makes logs unreadable, and ships nothing to a backend. Use `otlptracegrpc` / `otlpmetricgrpc` / `otlploggrpc` exporters, configured via standard env vars (`OTEL_EXPORTER_OTLP_ENDPOINT`, etc.).
- **P0 — Health & readiness.** No probes today.
  - Implement `grpc.health.v1.Health` and register it on the gRPC server.
  - Optionally also expose an HTTP `/healthz` (process up) and `/readyz` (ClickHouse ping OK + batcher not stuck) on a separate admin port.
- **P1 — Sampling.** `newTraceProvider()` uses `trace.AlwaysSample()`. At ingest scale this will swamp any tracing backend. Switch to `trace.ParentBased(trace.TraceIDRatioBased(...))` and make the ratio configurable.
- **P1 — Log level / format configuration.** `slog` is initialized with the `otelslog` bridge defaults and no level knob. Add `--log-level`, `--log-format=json|text`, and route through `otelslog` only in environments where logs are not also captured by container stdout.
- **P1 — Process-level metrics.** Add `runtime/metrics` (Go heap, goroutines, GC) and OS-level CPU/memory exporters; today only domain metrics are emitted.
- **P2 — Histogram buckets / exemplars.** `store.batch.latency` uses default histogram buckets; tune for the expected p99 (e.g. explicit boundaries from 1ms → 10s) and enable exemplars so a slow flush links back to the gRPC trace.
- **P2 — Cardinality of metric attributes.** `batchFailureReason` truncates `err.Error()` to 256 chars but still uses raw error text as an attribute value. Map errors to a small enum (`timeout`, `network`, `schema`, `auth`, `unknown`) before recording.

---

## 4. Configuration and packaging

- **P0 — Environment-variable config.** Only flags are supported (`cmd/server/main.go`). Add env-var fallbacks (`LISTEN_ADDR`, `STORE`, `CLICKHOUSE_ADDR`, `CLICKHOUSE_DATABASE`, `CLICKHOUSE_USERNAME`, `CLICKHOUSE_PASSWORD_FILE`, `LOG_LEVEL`, `OTEL_EXPORTER_OTLP_ENDPOINT`, …). Standard pattern: `viper`/`envconfig` or hand-rolled `os.Getenv`.
- **P0 — `flag.Parse()` ordering bug.** In `main.go`, `flag.Parse()` runs **after** `otelpipe.SetupOTelSDK(ctx)` and after the first `slog.Info(...)`. Today no flags affect those calls, but as soon as you add `--log-level` or `--otel-endpoint` it will silently be ignored. Move `flag.Parse()` to the very top of `main`.
- **P0 — Dockerfile + container image.** No `Dockerfile`, no `.dockerignore`, no multistage build. Add a multistage `golang:1.26 → distroless/static` image, set a non-root user, drop capabilities, and pin a known port (`EXPOSE 4317`).
- **P1 — Deploy manifests.** No Helm chart / Kustomize / K8s YAML in the tree. Provide a chart with: `Deployment` (rolling update with `terminationGracePeriodSeconds` ≥ shutdown flush timeout), `Service`, `ServiceMonitor`/`PodMonitor`, `NetworkPolicy`, `PodDisruptionBudget`, `HorizontalPodAutoscaler`, secrets references.
- **P1 — `docker-compose.yaml` for local dev.** Currently the README says “ensure ClickHouse is reachable” — give the user a one-shot `docker compose up` with the server + ClickHouse + (optionally) an OTel collector, and a small load generator service.
- **P2 — Build reproducibility.** `Makefile` targets are minimal. Add `-trimpath`, version stamping (`-ldflags "-X main.version=..."`), `CGO_ENABLED=0`, and an `info` flag/endpoint that exposes build info.

---

## 5. CI / CD

- **P0 — No CI in the repo.** Add a GitHub Actions (or equivalent) workflow that runs:
  - `go vet ./...`, `staticcheck ./...`, `golangci-lint` (errcheck, gosec, govet, unused, etc.).
  - `go test -race -count=1 ./...` and `go test -tags integration ./...` (testcontainers requires Docker on the runner).
  - `go mod verify`, `govulncheck`.
  - Container build + (optionally) `trivy`/`grype` scan.
- **P1 — Release pipeline.** Tag-driven release workflow that builds and pushes the image (signed with `cosign`), publishes `SBOM` (`syft`), and produces an SLSA provenance attestation.
- **P1 — Pre-commit / pre-push hooks.** A simple `lefthook` / `pre-commit` config to run `gofmt`, `go vet`, and a quick test subset locally.
- **P2 — Mutation / fuzz testing.** Add `go test -fuzz` for `internal/fingerprint` and `internal/otlpmap` (already non-trivially tested but fuzz would catch edge cases on attribute values, especially `anyValueToString`).

---

## 6. Mapper / OTLP coverage

- **P0 — Histograms, ExponentialHistograms, Summary are silently dropped.** `internal/otlpmap/mapper.go` only handles `Metric_Gauge` and `Metric_Sum`; any other instrument hits the `default:` branch with no log, no metric. At minimum:
  - Count dropped points by instrument kind and reason (so operators can see what their pipeline is losing).
  - Plan the schema for histograms (separate fact table or extra columns; the current scalar-only fact table cannot represent buckets).
- **P1 — `anyValueToString` is lossy.** Arrays, kvlists, and bytes fall through to `fmt.Sprintf("%v", v)`, which produces non-canonical, non-deterministic strings for the fingerprinter. Either:
  - Refuse to fingerprint complex values (and document it), or
  - Serialize them canonically (sorted JSON, length-prefixed) the way the rest of `fingerprint` does for maps.
- **P1 — Cardinality guard at the mapper.** A single misconfigured client can blow up the metadata table by including unique IDs in resource/datapoint attributes. Add:
  - A configurable max attribute count and max attribute value length per row.
  - A “high-cardinality detector” that emits a metric (`mapper.cardinality.warned`) when a metric name produces > N distinct fingerprints over a window.
- **P2 — Negative `IsMonotonic` semantics.** The fingerprint includes `IsMonotonic` and `AggregationTemporality`, which is correct. But the store does not validate temporality is set for sums; consider rejecting `AGGREGATION_TEMPORALITY_UNSPECIFIED` per OTLP spec.
- **P2 — Exemplars.** OTLP datapoints can carry exemplars (with span/trace IDs); these are dropped today. They’re very useful for drilling from a metric to a trace.
- **P2 — `sync.Pool` for hashers and maps.** As called out in `DECISIONS.md`, the hot path allocates a fresh `xxhash.Digest` and several small maps per datapoint. Pool them.

---

## 7. ClickHouse schema and write path

- **P0 — DDL TTL is hardcoded to 30 days.** `schema.go` has `TTL toDate(TimeUnix) + INTERVAL 30 DAY`. Extract to config so deployments can pick their retention; for compliance scenarios this needs to be auditable.
- **P0 — Migration story (see §2).** Versioned migrations are required to evolve the schema in production.
- **P1 — `wait_for_async_insert` default is conservative.** `buildSettings` defaults to `wait_for_async_insert = 1`. That gives read-after-write but caps writer throughput. Make it explicit at the deployment level (and document the trade-off in operator docs, not just `DECISIONS.md`).
- **P1 — Indexes on the metadata table may be excessive.** `idx_res_attr_key` / `idx_res_attr_value` are bloom filters over `mapKeys`/`mapValues`; for high-cardinality attribute maps they can hurt insert performance. Benchmark and turn off the ones not used by real queries.
- **P1 — Use `ReplicatedReplacingMergeTree` / `ReplicatedMergeTree`** in cluster deployments, with a `ZooKeeper`/Keeper path templated by config.
- **P1 — `OPTIMIZE … FINAL` strategy.** `ReplacingMergeTree` only deduplicates after merges. For latency-sensitive metadata reads add a documented periodic `OPTIMIZE` (or use `FINAL` selectively in queries) and surface the duplicate count as a metric.
- **P2 — Per-`Append` error handling.** In `store.UpsertMetadata` / `InsertDatapoints`, an `Append` error returns immediately without releasing the prepared batch. The `clickhouse-go` driver typically tolerates this, but explicitly aborting (`b.Abort()`) would be safer.
- **P2 — Partition strategy.** `PARTITION BY toDate(TimeUnix)` is fine for moderate volume; very large deployments may need monthly or sharded partitioning to keep parts manageable.
- **P2 — Per-table writer roles.** Once migrations are external, give the runtime user `INSERT` only on the two tables (no DDL, no `DROP`).

---

## 8. Multi-tenancy and scaling

- **P1 — Single global queue.** `store.Batcher` has one `metaCh` and one `dpCh` — a noisy service can fill both for everyone. Per-tenant (or per-fingerprint-shard) batchers would isolate failure.
- **P1 — Horizontal scaling.** With multiple replicas behind a load balancer, the LRU dedup in `Batcher` is per-process: every replica re-upserts the same metadata. Either:
  - Accept the cost (idempotent upsert into `ReplacingMergeTree`), or
  - Move the LRU to a shared cache (Redis), or
  - Route by `Fingerprint` (consistent hashing at the LB) so each replica owns a slice of the keyspace.
- **P2 — Backpressure semantics.** Today the gRPC handler returns `ResourceExhausted` and the *entire* batch is dropped. OTLP supports partial-success responses (`PartialSuccess.RejectedDataPoints`); returning success on what got queued and a partial-success on the rest is more cooperative.
- **P2 — OTLP/HTTP receiver.** Some clients can only export over HTTP/`protobuf`. Add an HTTP listener (`google.golang.org/grpc` cannot, but `net/http` plus the same `otlpmap.MapRequest` works fine).

---

## 9. Code quality and small fixes

- **P1 — `internal/grpcserver/service.go` log message.** `"export metrics: backpressure, queue full"` includes the error twice (in the message and in `slog.String("err", err.Error())`). Use `slog.Any("err", err)` and drop the literal in the message, or use OTel error semconv (`error.type`, `error.message`).
- **P1 — `Run` shutdown error joining.** `errors.Join(flushErr, closeErr, serveErr)` where `serveErr` after `GracefulStop()` is typically `nil` or `grpc.ErrServerStopped`. Filter known-benign errors so the process exit code reflects real failures.
- **P1 — `applyBatcherDefaults` treats `0` as “unset”.** This makes it impossible to configure `MaxBatchRetries = 0` (i.e. fail fast). Switch to pointer fields or a sentinel (`-1` = disabled).
- **P2 — `cloneMap` returns an empty map for `nil`.** Forces ClickHouse `Map(...)` columns to always have a value, but allocates an empty map per row. The driver accepts a `nil` map; benchmark and drop the allocation if so.
- **P2 — Hardcoded shutdown flush timeout (30s).** Promote to a flag/env var; a slow ClickHouse may need 1–2 minutes of drain on rolling restart.
- **P2 — Service name / namespace baked into `otelpipe.setup.go`.** `service.name = "otlp-metrics-processor-backend"` and `service.namespace = "dash0-exercise"` are hardcoded. Read them from env / config.

---

## 10. Testing and load

- **P1 — Race detector in CI.** Add `-race` to the standard test target; the batcher has explicit concurrency that should be exercised.
- **P1 — End-to-end load test.** No reproducible benchmark today. Add a small `cmd/loadgen` (or a `ghz` script) that generates a configurable mix of gauge + sum + duplicate-fingerprint traffic and asserts on backpressure rate, p99 flush latency, and ClickHouse part counts.
- **P1 — Chaos test for the batcher.** A test that injects intermittent ClickHouse failures (already partially supported via `WithFailingWriteCalls` on the memory store) and asserts the buffer doesn’t grow without bound.
- **P2 — Mapper fuzz.** Property-based / fuzz tests for `MapRequest` to make sure malformed OTLP doesn’t panic.

---

## 11. Documentation

- **P1 — Runbook.** What to do when `queue.dropped` is increasing, when `store.batch.failed` is non-zero, when ClickHouse merges back up. Today only `README.md` covers the happy path.
- **P2 — Dashboard JSON.** Ship a vendor-agnostic dashboard definition pinned to the metric names in `internal/store/otel_batcher.go` and `internal/grpcserver/export_otel.go` so first-time operators have something to import into their visualization tool.
- **P2 — Capacity / sizing notes.** Suggested `SizeFlushThreshold`, `MaxMetadataChannel`, `MaxDatapointChannel` defaults vs. RPS; how fingerprint cardinality affects memory in the LRU.

---

## Summary — minimum cut to call this “v1 prod”

If forced to pick the smallest meaningful slice before going live, it would be:

1. TLS + bearer-token auth on the gRPC listener.
2. Secrets via env / file, not flags.
3. Dockerfile + minimal Helm/K8s manifests with health probes and graceful termination.
4. OTLP (not stdout) exporters for traces / metrics / logs.
5. External, versioned migrations (drop `CreateTables` from server boot).
6. CI with `vet` + `staticcheck` + race tests + `govulncheck`.
7. Bounded accumulator + alert on `store.batch.failed` + dead-letter sink.
8. Drop `flag.Parse()` ordering bug; promote service name / shutdown timeout / log level / OTel endpoint to config.

Everything else in this document is incremental hardening on top of that base.
