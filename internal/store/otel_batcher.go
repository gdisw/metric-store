package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const OTelScopeName = "dash0.com/otlp-log-processor-backend"

// BatcherMetrics holds OpenTelemetry instruments for the async batcher and its queues.
// Use [NewBatcherMetrics] to construct; a nil *BatcherMetrics is safe where batcher
// does not record metrics.
type BatcherMetrics struct {
	metadataCacheHit  metric.Int64Counter
	metadataCacheMiss metric.Int64Counter
	batchInserted     metric.Int64Counter
	batchFailed       metric.Int64Counter
	batchLatency      metric.Float64Histogram
	queueDropped      metric.Int64Counter
}

// NewBatcherMetrics registers batcher–related instruments for the given meter.
func NewBatcherMetrics(m metric.Meter) (*BatcherMetrics, error) {
	var errs error

	hit, err := m.Int64Counter("metadata.cache.hit",
		metric.WithDescription("Metadata LRU dedup: fingerprint was already in cache"),
		metric.WithUnit("1"),
	)
	if err != nil {
		errs = errors.Join(errs, fmt.Errorf("metadata.cache.hit: %w", err))
	}

	miss, err := m.Int64Counter("metadata.cache.miss",
		metric.WithDescription("Metadata LRU dedup: fingerprint not in cache, metadata queued"),
		metric.WithUnit("1"),
	)
	if err != nil {
		errs = errors.Join(errs, fmt.Errorf("metadata.cache.miss: %w", err))
	}

	inserted, err := m.Int64Counter("store.batch.inserted",
		metric.WithDescription("Rows successfully written in a store batch operation"),
		metric.WithUnit("1"),
	)
	if err != nil {
		errs = errors.Join(errs, fmt.Errorf("store.batch.inserted: %w", err))
	}

	failed, err := m.Int64Counter("store.batch.failed",
		metric.WithDescription("Batched store operation failed after retries"),
		metric.WithUnit("1"),
	)
	if err != nil {
		errs = errors.Join(errs, fmt.Errorf("store.batch.failed: %w", err))
	}

	lat, err := m.Float64Histogram("store.batch.latency",
		metric.WithDescription("Duration of a single store batch call (metadata upsert or datapoint insert)"),
		metric.WithUnit("s"),
	)
	if err != nil {
		errs = errors.Join(errs, fmt.Errorf("store.batch.latency: %w", err))
	}

	drop, err := m.Int64Counter("queue.dropped",
		metric.WithDescription("Rows dropped when batcher channels are full (backpressure)"),
		metric.WithUnit("1"),
	)
	if err != nil {
		errs = errors.Join(errs, fmt.Errorf("queue.dropped: %w", err))
	}

	if errs != nil {
		return nil, errs
	}

	return &BatcherMetrics{
		metadataCacheHit:  hit,
		metadataCacheMiss: miss,
		batchInserted:     inserted,
		batchFailed:       failed,
		batchLatency:      lat,
		queueDropped:      drop,
	}, nil
}

// RegisterQueueDepthCallback registers a periodic [queue.depth] measurement for
// both internal channels. The returned [metric.Registration] should be
// [metric.Registration.Unregister] on shutdown, or the meter provider can own its lifetime.
func RegisterQueueDepthCallback(m metric.Meter, b *Batcher) (metric.Registration, error) {
	g, err := m.Int64ObservableGauge("queue.depth",
		metric.WithDescription("Count of items waiting in the batcher metadata and datapoint channels"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}
	return m.RegisterCallback(
		func(ctx context.Context, o metric.Observer) error {
			mq, dq := b.ChannelDepths()
			o.ObserveInt64(g, int64(mq), metric.WithAttributes(
				attribute.String("queue", "metadata"),
			))
			o.ObserveInt64(g, int64(dq), metric.WithAttributes(
				attribute.String("queue", "datapoints"),
			))
			return nil
		},
		g,
	)
}

// OnCacheHit records an LRU hit for a metadata row (skip metadata queue).
func (b *BatcherMetrics) OnCacheHit(ctx context.Context) {
	if b == nil {
		return
	}
	b.metadataCacheHit.Add(ctx, 1)
}

// OnCacheMiss records an LRU miss: metadata is sent to the channel.
func (b *BatcherMetrics) OnCacheMiss(ctx context.Context) {
	if b == nil {
		return
	}
	b.metadataCacheMiss.Add(ctx, 1)
}

// OnBatchInserted records successfully written rows; kind is "metadata" or "datapoints".
func (b *BatcherMetrics) OnBatchInserted(ctx context.Context, kind string, n int64) {
	if b == nil || n <= 0 {
		return
	}
	b.batchInserted.Add(ctx, n, metric.WithAttributes(
		attribute.String("kind", kind),
	))
}

// OnBatchFailed records a final failure after retries; kind is "metadata" or "datapoints".
func (b *BatcherMetrics) OnBatchFailed(ctx context.Context, kind string, err error) {
	if b == nil || err == nil {
		return
	}
	b.batchFailed.Add(ctx, 1, metric.WithAttributes(
		attribute.String("kind", kind),
		attribute.String("reason", batchFailureReason(err)),
	))
}

// OnBatchLatency records the duration of one successful store call; kind is "metadata" or "datapoints".
func (b *BatcherMetrics) OnBatchLatency(ctx context.Context, kind string, d time.Duration) {
	if b == nil {
		return
	}
	if d < 0 {
		d = 0
	}
	b.batchLatency.Record(ctx, d.Seconds(), metric.WithAttributes(
		attribute.String("kind", kind),
	))
}

// OnQueueDropped records rows not accepted under backpressure.
func (b *BatcherMetrics) OnQueueDropped(ctx context.Context, n int64) {
	if b == nil || n <= 0 {
		return
	}
	b.queueDropped.Add(ctx, n)
}

func batchFailureReason(err error) string {
	if err == nil {
		return "unknown"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	s := err.Error()
	const max = 256
	if len(s) > max {
		return s[:max]
	}
	return s
}
