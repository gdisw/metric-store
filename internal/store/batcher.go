package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

// ErrBackpressure is returned when an internal queue is full (non-blocking send).
var ErrBackpressure = errors.New("store batcher: queue full")

// Batcher batches rows to a [MetricsStore], with LRU metadata dedup after successful upserts.
type Batcher struct {
	store  MetricsStore
	cfg    BatcherConfig
	closed atomic.Bool
	drop   atomic.Uint64

	lru *lru.Cache[uint64, struct{}]

	enqueueMu sync.Mutex

	metaCh chan MetadataRow
	dpCh   chan DatapointRow
	stopCh chan struct{}
	done   chan struct{}

	flushErr atomic.Value
}

// BatcherConfig. Zero values use [DefaultBatcherConfig] in [NewBatcher].
type BatcherConfig struct {
	MaxMetadataChannel  int
	MaxDatapointChannel int
	SizeFlushThreshold  int
	FlushInterval       time.Duration
	LRUMaxEntries       int
	MaxBatchRetries     int
}

func DefaultBatcherConfig() BatcherConfig {
	return BatcherConfig{
		MaxMetadataChannel:  10_000,
		MaxDatapointChannel: 10_000,
		SizeFlushThreshold:  5_000,
		FlushInterval:       time.Second,
		LRUMaxEntries:       100_000,
		MaxBatchRetries:     3,
	}
}

func applyBatcherDefaults(cfg BatcherConfig) BatcherConfig {
	d := DefaultBatcherConfig()
	if cfg.MaxMetadataChannel == 0 {
		cfg.MaxMetadataChannel = d.MaxMetadataChannel
	}
	if cfg.MaxDatapointChannel == 0 {
		cfg.MaxDatapointChannel = d.MaxDatapointChannel
	}
	if cfg.SizeFlushThreshold == 0 {
		cfg.SizeFlushThreshold = d.SizeFlushThreshold
	}
	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = d.FlushInterval
	}
	if cfg.LRUMaxEntries == 0 {
		cfg.LRUMaxEntries = d.LRUMaxEntries
	}
	if cfg.MaxBatchRetries == 0 {
		cfg.MaxBatchRetries = d.MaxBatchRetries
	}
	return cfg
}

func NewBatcher(store MetricsStore, cfg BatcherConfig) (*Batcher, error) {
	if store == nil {
		return nil, errors.New("store batcher: nil store")
	}
	cfg = applyBatcherDefaults(cfg)
	if cfg.LRUMaxEntries < 1 {
		return nil, errors.New("store batcher: LRUMaxEntries must be >= 1")
	}
	cache, err := lru.New[uint64, struct{}](cfg.LRUMaxEntries)
	if err != nil {
		return nil, fmt.Errorf("store batcher: lru: %w", err)
	}
	b := &Batcher{
		store:  store,
		cfg:    cfg,
		lru:    cache,
		metaCh: make(chan MetadataRow, cfg.MaxMetadataChannel),
		dpCh:   make(chan DatapointRow, cfg.MaxDatapointChannel),
		stopCh: make(chan struct{}),
		done:   make(chan struct{}),
	}
	go b.run()
	return b, nil
}

// Dropped counts rows not queued due to [ErrBackpressure].
func (b *Batcher) Dropped() uint64 {
	return b.drop.Load()
}

// Enqueue may skip metadata already in the LRU. Full queue: [ErrBackpressure].
// A call either queues every row that still needs the channels (all-or-nothing for
// this batch) or returns [ErrBackpressure] without leaving a prefix queued, so
// retries do not duplicate datapoints.
func (b *Batcher) Enqueue(ctx context.Context, metadata []MetadataRow, datapoints []DatapointRow) error {
	if b.closed.Load() {
		return errors.New("store batcher: closed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	b.enqueueMu.Lock()
	defer b.enqueueMu.Unlock()

	if b.closed.Load() {
		return errors.New("store batcher: closed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	needMeta := 0
	for i := range metadata {
		if _, hit := b.lru.Peek(metadata[i].Fingerprint); !hit {
			needMeta++
		}
	}
	needDp := len(datapoints)

	if needMeta > cap(b.metaCh)-len(b.metaCh) || needDp > cap(b.dpCh)-len(b.dpCh) {
		b.drop.Add(uint64(len(metadata) + len(datapoints)))
		return ErrBackpressure
	}

	for i := range metadata {
		row := metadata[i]
		if _, hit := b.lru.Peek(row.Fingerprint); hit {
			continue
		}
		b.metaCh <- row
	}
	for i := range datapoints {
		b.dpCh <- datapoints[i]
	}
	return nil
}

func (b *Batcher) run() {
	defer close(b.done)
	cfg := b.cfg
	tick := time.NewTicker(cfg.FlushInterval)
	defer tick.Stop()

	var metaAcc []MetadataRow
	var dpAcc []DatapointRow
	ctx := context.Background()

	for {
		select {
		case m := <-b.metaCh:
			metaAcc = append(metaAcc, m)
			if len(metaAcc)+len(dpAcc) >= cfg.SizeFlushThreshold {
				b.onFlushError(b.doFlush(ctx, &metaAcc, &dpAcc))
			}
		case d := <-b.dpCh:
			dpAcc = append(dpAcc, d)
			if len(metaAcc)+len(dpAcc) >= cfg.SizeFlushThreshold {
				b.onFlushError(b.doFlush(ctx, &metaAcc, &dpAcc))
			}
		case <-tick.C:
			b.onFlushError(b.doFlush(ctx, &metaAcc, &dpAcc))
		case <-b.stopCh:
			b.shutdownFlush(ctx, &metaAcc, &dpAcc)
			return
		}
	}
}

func (b *Batcher) shutdownFlush(ctx context.Context, metaAcc *[]MetadataRow, dpAcc *[]DatapointRow) {
	for {
		b.drainChansNonblock(metaAcc, dpAcc)
		if len(*metaAcc) == 0 && len(*dpAcc) == 0 && len(b.metaCh) == 0 && len(b.dpCh) == 0 {
			return
		}
		if err := b.doFlush(ctx, metaAcc, dpAcc); err != nil {
			b.setFlushErr(err)
			return
		}
	}
}

func (b *Batcher) drainChansNonblock(metaAcc *[]MetadataRow, dpAcc *[]DatapointRow) {
	for {
		select {
		case m := <-b.metaCh:
			*metaAcc = append(*metaAcc, m)
		case d := <-b.dpCh:
			*dpAcc = append(*dpAcc, d)
		default:
			return
		}
	}
}

func (b *Batcher) onFlushError(err error) {
	if err != nil {
		b.setFlushErr(err)
	}
}

// Flush stops the worker, drains, and flushes. Idempotent.
// If ctx is cancelled or times out before the worker exits, Flush still blocks until
// the worker goroutine has finished (so callers may Close the store safely afterward).
// Any context error is joined with a failed flush error from the store, if any.
func (b *Batcher) Flush(ctx context.Context) error {
	if !b.closed.Swap(true) {
		close(b.stopCh)
	}
	var ctxErr error
	select {
	case <-b.done:
	case <-ctx.Done():
		ctxErr = ctx.Err()
		<-b.done
	}
	var flushErr error
	if v := b.flushErr.Load(); v != nil {
		if err, _ := v.(error); err != nil {
			flushErr = err
		}
	}
	return errors.Join(ctxErr, flushErr)
}

func (b *Batcher) setFlushErr(err error) {
	if err != nil {
		b.flushErr.Store(err)
	}
}

func (b *Batcher) doFlush(ctx context.Context, metaAcc *[]MetadataRow, dpAcc *[]DatapointRow) error {
	if len(*metaAcc) == 0 && len(*dpAcc) == 0 {
		return nil
	}
	meta := append([]MetadataRow(nil), *metaAcc...)
	dps := append([]DatapointRow(nil), *dpAcc...)

	*metaAcc = (*metaAcc)[:0]
	*dpAcc = (*dpAcc)[:0]

	if err := b.writeBatchesWithRetry(ctx, meta, dps); err != nil {
		*metaAcc = append(meta, *metaAcc...)
		*dpAcc = append(dps, *dpAcc...)
		return err
	}
	return nil
}

func (b *Batcher) writeBatchesWithRetry(ctx context.Context, meta []MetadataRow, dps []DatapointRow) error {
	retries := b.cfg.MaxBatchRetries
	remainingMeta := meta
	remainingDps := dps

	for attempt := 0; attempt < retries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(remainingMeta) > 0 {
			if err := b.store.UpsertMetadata(ctx, remainingMeta); err != nil {
				if isNonRetriableErr(err) {
					return fmt.Errorf("store batcher: UpsertMetadata: %w", err)
				}
				if attempt == retries-1 {
					return fmt.Errorf("store batcher: UpsertMetadata: %w", err)
				}
				b.backoff(ctx, attempt)
				continue
			}
			for _, r := range remainingMeta {
				b.lru.Add(r.Fingerprint, struct{}{})
			}
			remainingMeta = nil
		}
		if len(remainingDps) > 0 {
			if err := b.store.InsertDatapoints(ctx, remainingDps); err != nil {
				if isNonRetriableErr(err) {
					return fmt.Errorf("store batcher: InsertDatapoints: %w", err)
				}
				if attempt == retries-1 {
					return fmt.Errorf("store batcher: InsertDatapoints: %w", err)
				}
				b.backoff(ctx, attempt)
				continue
			}
			remainingDps = nil
		}
		if len(remainingMeta) == 0 && len(remainingDps) == 0 {
			return nil
		}
	}
	return errors.New("store batcher: writeBatchesWithRetry: exhausted with remaining work")
}

func isNonRetriableErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func (b *Batcher) backoff(ctx context.Context, attempt int) {
	if attempt >= b.cfg.MaxBatchRetries-1 {
		return
	}
	shift := min(attempt, 20)
	d := time.Duration(1<<shift) * time.Millisecond
	if d > 2*time.Second {
		d = 2 * time.Second
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
