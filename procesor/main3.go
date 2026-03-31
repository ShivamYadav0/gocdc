package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

var (
	// ==================== CORE METRICS ====================

	messagesProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "nms_processor_messages_total",
		Help: "Total number of Kafka messages processed",
	}, []string{"topic", "status"})

	processingErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "nms_processor_errors_total",
		Help: "Total processing errors",
	}, []string{"type", "operation"})

	kafkaMessageLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "nms_processor_kafka_message_duration_seconds",
		Help:    "Time spent processing single Kafka message end-to-end",
		Buckets: prometheus.DefBuckets,
	})

	// ==================== WORKER POOL ====================

	// Tracks how many of the WorkerCount goroutines are actively processing (not idle waiting on Kafka)
	activeWorkers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "nms_processor_workers_active",
		Help: "Number of worker goroutines currently processing a message (not idle)",
	})

	// Saturation = activeWorkers / WorkerCount — 1.0 means all workers are pegged
	workerPoolSaturation = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "nms_processor_worker_pool_saturation_ratio",
		Help: "Fraction of workers that are busy (0.0–1.0). At 1.0 you are fully saturated.",
	})

	// ==================== CHANNEL / BACKPRESSURE ====================

	// Per-shard channel depth — tracks each of the ShardCount interval channels
	shardChannelDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "nms_event_channel_depth",
		Help: "Current number of IntervalRecords queued in each batcher shard channel",
	}, []string{"shard"})

	// Absolute saturation 0.0–1.0 across ALL shard channels combined
	channelUtilization = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "nms_event_channel_utilization",
		Help: "Overall channel fill ratio (0.0–1.0). 1.0 = full backpressure / HoL blocking imminent.",
	})

	// How many times a worker had to block because every shard channel was full
	channelBlockedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "nms_event_channel_blocked_total",
		Help: "Number of times a worker goroutine blocked trying to enqueue an IntervalRecord (backpressure events).",
	})

	// ==================== CLICKHOUSE BATCHER ====================

	clickhouseBatchLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "nms_clickhouse_batch_duration_seconds",
		Help:    "ClickHouse batch insert latency per shard",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
	}, []string{"shard"})

	clickhouseBatchSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "nms_clickhouse_batch_size_records",
		Help:    "Number of records flushed in each ClickHouse batch per shard",
		Buckets: []float64{100, 500, 1000, 2000, 5000, 10000},
	}, []string{"shard"})

	// Timeout counter — the KEY metric that catches a hanging INSERT
	clickhouseTimeouts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "nms_clickhouse_insert_timeouts_total",
		Help: "Number of ClickHouse INSERT operations that exceeded the context deadline (context.WithTimeout). " +
			"A non-zero rate here indicates Part Merge pressure or insert_quorum slowness.",
	}, []string{"shard"})

	// Tracks the wall-clock time of the most recently successfully committed batch
	// Diff with time() in PromQL gives you ingestion lag
	clickhouseLastCommitTimestamp = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "nms_clickhouse_last_commit_timestamp_seconds",
		Help: "Unix timestamp of the most recent successful ClickHouse batch commit per shard. " +
			"Use `time() - nms_clickhouse_last_commit_timestamp_seconds` to measure ingestion lag.",
	}, []string{"shard"})

	// How many rows are currently buffered waiting to be flushed
	clickhouseBufferDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "nms_clickhouse_buffer_depth",
		Help: "Number of IntervalRecords currently sitting in the in-memory batcher buffer (pre-flush).",
	}, []string{"shard"})

	// ==================== REDIS ====================

	redisOperationDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "nms_redis_operation_duration_seconds",
		Help:    "Redis operation latency by operation type",
		Buckets: prometheus.DefBuckets,
	}, []string{"operation"})

	// Redis pipeline/connection errors distinct from processing errors
	redisErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "nms_redis_errors_total",
		Help: "Total Redis errors by operation (GET/SET/pipeline) — helps distinguish Redis vs ClickHouse failures.",
	}, []string{"operation"})

	// Cache-miss rate: how often node_id is NOT in Redis (cold start / TTL expiry)
	redisCacheMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "nms_redis_cache_misses_total",
		Help: "Number of Redis GETs that returned a miss (key not found). High rate after restart is normal.",
	})

	// ==================== BUSINESS / DOMAIN METRICS ====================

	nodeStateTransitions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "nms_node_state_transitions_total",
		Help: "Observed node UP↔DOWN state transitions",
	}, []string{"node_id", "from_state", "to_state"})

	activeDownNodes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "nms_active_down_nodes",
		Help: "Current number of nodes in DOWN state (derived from Redis state tracking).",
	})

	intervalRecordsCreated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "nms_interval_records_total",
		Help: "Total DOWN→UP interval records generated and enqueued for ClickHouse persistence.",
	})

	// Duration distribution of observed outages — lets you build SLO histograms
	intervalDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "nms_interval_duration_seconds",
		Help:    "Distribution of node outage durations (seconds between DOWN and UP events).",
		Buckets: []float64{5, 15, 30, 60, 120, 300, 600, 1800, 3600},
	})

	// Identifies nodes that flap (DOWN→UP→DOWN very quickly) — operational noise signal
	nodeFlaps = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "nms_node_flaps_total",
		Help: "Number of times a node transitioned DOWN→UP in under FlapThreshold seconds (likely a flapping node).",
	}, []string{"node_id"})

	// ==================== KAFKA CONSUMER ====================

	workerQueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "nms_worker_queue_depth",
		Help: "Alias kept for backwards compatibility — prefer nms_event_channel_depth per shard.",
	})

	kafkaLagSeconds = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "nms_kafka_consumer_lag_seconds",
		Help: "Estimated Kafka consumer lag in seconds (wall-clock delta between message timestamp and now).",
	}, []string{"topic", "group"})

	// Raw offset lag (needs external exporter for full accuracy, but we track message-timestamp delta here)
	kafkaMessagesSkipped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "nms_kafka_messages_skipped_total",
		Help: "Messages discarded before processing (empty node_id, schema mismatch, etc.).",
	}, []string{"reason"})

	// ==================== PIPELINE HEALTH / E2E ====================

	// End-to-end latency: from Kafka message timestamp to the moment the interval is written to ClickHouse
	e2eLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "nms_pipeline_e2e_latency_seconds",
		Help:    "End-to-end latency: Kafka event_time → ClickHouse commit. Captures full pipeline delay.",
		Buckets: []float64{0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60},
	})

	// Data freshness gauge — set to `time.Now().Unix()` after each successful ClickHouse flush
	// Grafana alert: time() - nms_pipeline_last_event_processed_timestamp > 60  → page
	lastEventProcessedTimestamp = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "nms_pipeline_last_event_processed_timestamp_seconds",
		Help: "Unix timestamp of the last event that completed the full pipeline (Kafka→Redis→CH). " +
			"Staleness of this gauge indicates a silent pipeline freeze.",
	})
)

const (
	KafkaBroker    = "localhost:9093"
	KafkaTopic     = "dbserver1.inventory.histalarms"
	KafkaGroup     = "nms-processor-v1"
	RedisAddr      = "localhost:6379"
	ClickHouseAddr = "localhost:9000"

	WorkerCount     = 20
	BatchSize       = 2000
	FlushInterval   = 2 * time.Second
	ShardCount      = 5                      // number of parallel batcher goroutines
	ShardChanSize   = 4000                   // per-shard channel capacity  (total = 20k)
	CHInsertTimeout = 2 * time.Second        // context deadline for each ClickHouse INSERT
	FlapThreshold   = 10 * time.Second       // outage shorter than this → counted as flap
)

var (
	msgCounter      int64
	intervalCounter int64
	logger          = logrus.New()
)

// ─── Data Structures ──────────────────────────────────────────────────────────

type DebeziumPayload struct {
	Payload struct {
		After struct {
			NodeID    string `json:"node_id"`
			TrapID    int    `json:"trap_id"`
			EventTime int64  `json:"event_time"`
		} `json:"after"`
	} `json:"payload"`
}

type NodeState struct {
	Status     string    `json:"status"`
	LastSeenAt time.Time `json:"last_seen_at"`
}

type IntervalRecord struct {
	NodeID      string
	DownAt      time.Time
	UpAt        time.Time
	DurationSec float64
	// KafkaEnqueuedAt is set when the record is created so we can track E2E latency
	KafkaEnqueuedAt time.Time
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	logger.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Redis
	rdb := redis.NewClient(&redis.Options{
		Addr:         RedisAddr,
		PoolSize:     100,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	})

	// ClickHouse — one connection pool shared across shards via multiple Open() calls
	chConns := make([]clickhouse.Conn, ShardCount)
	for i := 0; i < ShardCount; i++ {
		conn, err := clickhouse.Open(&clickhouse.Options{
			Addr: []string{ClickHouseAddr},
			Auth: clickhouse.Auth{Database: "default"},
		})
		if err != nil {
			logger.Fatalf("ClickHouse connect failed for shard %d: %v", i, err)
		}
		chConns[i] = conn
	}

	// Kafka reader (single reader; workers share it via ReadMessage which is goroutine-safe)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{KafkaBroker},
		Topic:    KafkaTopic,
		GroupID:  KafkaGroup,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	// ShardCount independent interval channels
	shardChans := make([]chan IntervalRecord, ShardCount)
	for i := 0; i < ShardCount; i++ {
		shardChans[i] = make(chan IntervalRecord, ShardChanSize)
	}

	var wg sync.WaitGroup

	logger.Infof("NMS Processor started | Workers: %d | Shards: %d | BatchSize: %d | CHTimeout: %s",
		WorkerCount, ShardCount, BatchSize, CHInsertTimeout)

	// Prometheus metrics endpoint
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		logger.Infof("Prometheus metrics at :2112/metrics")
		if err := http.ListenAndServe(":2112", nil); err != nil {
			logger.Errorf("Metrics server error: %v", err)
		}
	}()

	// One batcher goroutine per shard
	batcherDone := make(chan struct{})
	var batchWg sync.WaitGroup
	for i := 0; i < ShardCount; i++ {
		batchWg.Add(1)
		go func(shardID int) {
			defer batchWg.Done()
			runClickHouseBatcher(ctx, shardID, chConns[shardID], shardChans[shardID])
		}(i)
	}
	go func() {
		batchWg.Wait()
		close(batcherDone)
	}()

	// Worker goroutines — each worker picks the shard by hash(node_id) % ShardCount
	for i := 0; i < WorkerCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			processKafkaStream(ctx, id, reader, rdb, shardChans)
		}(i)
	}

	// Background monitoring ticker
	go monitoringRoutine(ctx, shardChans)

	<-ctx.Done()
	logger.Warn("Shutdown signal received — draining...")
	reader.Close()
	wg.Wait()
	for i := 0; i < ShardCount; i++ {
		close(shardChans[i])
	}
	<-batcherDone
	logger.Info("Clean shutdown completed.")
}

// ─── Worker ───────────────────────────────────────────────────────────────────

func processKafkaStream(
	ctx context.Context,
	workerID int,
	r *kafka.Reader,
	rdb *redis.Client,
	shards []chan IntervalRecord,
) {
	for {
		msgStart := time.Now()

		// Mark worker as active (subtract on next iteration start)
		activeWorkers.Inc()

		m, err := r.ReadMessage(ctx)
		if err != nil {
			activeWorkers.Dec()
			return
		}

		atomic.AddInt64(&msgCounter, 1)
		kafkaMessageLatency.Observe(time.Since(msgStart).Seconds())

		// Track Kafka lag (message timestamp vs wall clock)
		lag := time.Since(m.Time).Seconds()
		if lag > 0 {
			kafkaLagSeconds.WithLabelValues(KafkaTopic, KafkaGroup).Set(lag)
		}

		var env DebeziumPayload
		if err := json.Unmarshal(m.Value, &env); err != nil {
			processingErrors.WithLabelValues("json_unmarshal", "kafka").Inc()
			activeWorkers.Dec()
			continue
		}

		data := env.Payload.After
		if data.NodeID == "" {
			kafkaMessagesSkipped.WithLabelValues("empty_node_id").Inc()
			activeWorkers.Dec()
			continue
		}

		messagesProcessed.WithLabelValues(KafkaTopic, "ok").Inc()

		eventTime := time.UnixMilli(data.EventTime)
		currentStatus := "UP"
		if data.TrapID == 1417 || data.TrapID == 1535 {
			currentStatus = "DOWN"
		}

		key := fmt.Sprintf("node:%s", data.NodeID)

		// ── Redis GET ──────────────────────────────────────────────────────────
		redisStart := time.Now()
		val, redisErr := rdb.Get(ctx, key).Result()
		redisOperationDuration.WithLabelValues("GET").Observe(time.Since(redisStart).Seconds())

		var prevStatus string
		if redisErr != nil {
			if redisErr != redis.Nil {
				redisErrors.WithLabelValues("GET").Inc()
				processingErrors.WithLabelValues("redis", "GET").Inc()
			} else {
				// Key not found — cold start or TTL expiry
				redisCacheMisses.Inc()
			}
		} else {
			var prev NodeState
			if json.Unmarshal([]byte(val), &prev) == nil {
				prevStatus = prev.Status

				if prev.Status == "DOWN" && currentStatus == "UP" {
					dur := eventTime.Sub(prev.LastSeenAt).Seconds()
					if dur > 0 {
						rec := IntervalRecord{
							NodeID:          data.NodeID,
							DownAt:          prev.LastSeenAt,
							UpAt:            eventTime,
							DurationSec:     dur,
							KafkaEnqueuedAt: time.Now(),
						}

						// Record outage duration in histogram
						intervalDurationSeconds.Observe(dur)

						// Detect flapping nodes
						if dur < FlapThreshold.Seconds() {
							nodeFlaps.WithLabelValues(data.NodeID).Inc()
						}

						// Route to shard by node_id hash for Head-of-Line isolation
						shardIdx := shardIndex(data.NodeID)
						shard := shards[shardIdx]

						// Non-blocking send with backpressure tracking
						select {
						case shard <- rec:
							// happy path
						default:
							// Channel is full — record the backpressure event, then block
							channelBlockedTotal.Inc()
							shard <- rec // block until space frees up
						}

						atomic.AddInt64(&intervalCounter, 1)
						intervalRecordsCreated.Inc()
						nodeStateTransitions.WithLabelValues(data.NodeID, "DOWN", "UP").Inc()
						lastEventProcessedTimestamp.SetToCurrentTime()
					}
				}
			}
		}

		// ── Redis SET ──────────────────────────────────────────────────────────
		newState, _ := json.Marshal(NodeState{Status: currentStatus, LastSeenAt: eventTime})
		redisStart = time.Now()
		if err := rdb.Set(ctx, key, newState, 48*time.Hour).Err(); err != nil {
			redisErrors.WithLabelValues("SET").Inc()
			processingErrors.WithLabelValues("redis", "SET").Inc()
		}
		redisOperationDuration.WithLabelValues("SET").Observe(time.Since(redisStart).Seconds())

		// State transition tracking (DOWN direction)
		if currentStatus == "DOWN" && prevStatus != "DOWN" {
			nodeStateTransitions.WithLabelValues(data.NodeID, prevStatus, "DOWN").Inc()
			activeDownNodes.Inc()
		} else if currentStatus == "UP" && prevStatus == "DOWN" {
			activeDownNodes.Dec()
		}

		activeWorkers.Dec()
	}
}

// shardIndex returns a consistent shard index for a given node_id string
// using a simple FNV-inspired hash — avoids import of crypto/hash.
func shardIndex(nodeID string) int {
	h := uint32(2166136261)
	for i := 0; i < len(nodeID); i++ {
		h ^= uint32(nodeID[i])
		h *= 16777619
	}
	return int(h) % ShardCount
}

// ─── ClickHouse Batcher (one per shard) ───────────────────────────────────────

func runClickHouseBatcher(
	ctx context.Context,
	shardID int,
	ch clickhouse.Conn,
	in <-chan IntervalRecord,
) {
	shardLabel := fmt.Sprintf("%d", shardID)
	buffer := make([]IntervalRecord, 0, BatchSize)
	ticker := time.NewTicker(FlushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(buffer) == 0 {
			return
		}

		// ── KEY FIX: context.WithTimeout prevents zombie INSERT hanging all workers ──
		insertCtx, cancel := context.WithTimeout(context.Background(), CHInsertTimeout)
		defer cancel()

		start := time.Now()

		batch, err := ch.PrepareBatch(insertCtx, "INSERT INTO node_intervals (node_id, down_at, up_at, duration_sec)")
		if err != nil {
			processingErrors.WithLabelValues("clickhouse", "prepare").Inc()
			clickhouseErrors.WithLabelValues(shardLabel, "prepare").Inc()
			// Don't discard buffer; we'll retry on next tick
			return
		}

		for _, rec := range buffer {
			_ = batch.Append(rec.NodeID, rec.DownAt, rec.UpAt, rec.DurationSec)
		}

		if err := batch.Send(); err != nil {
			// Distinguish timeout vs other error
			if insertCtx.Err() == context.DeadlineExceeded {
				clickhouseTimeouts.WithLabelValues(shardLabel).Inc()
				logger.Warnf("[shard %d] ClickHouse INSERT timed out after %s — buffer preserved for retry", shardID, CHInsertTimeout)
			} else {
				processingErrors.WithLabelValues("clickhouse", "send").Inc()
				clickhouseErrors.WithLabelValues(shardLabel, "send").Inc()
			}
			return // preserve buffer for retry — do NOT clear it
		}

		// Successful flush
		elapsed := time.Since(start).Seconds()
		clickhouseBatchLatency.WithLabelValues(shardLabel).Observe(elapsed)
		clickhouseBatchSize.WithLabelValues(shardLabel).Observe(float64(len(buffer)))
		clickhouseLastCommitTimestamp.WithLabelValues(shardLabel).SetToCurrentTime()

		// Record E2E latency for each record in the batch
		now := time.Now()
		for _, rec := range buffer {
			e2eLatency.Observe(now.Sub(rec.KafkaEnqueuedAt).Seconds())
		}

		buffer = buffer[:0]
		clickhouseBufferDepth.WithLabelValues(shardLabel).Set(0)
	}

	for {
		select {
		case rec, ok := <-in:
			if !ok {
				flush()
				return
			}
			buffer = append(buffer, rec)
			clickhouseBufferDepth.WithLabelValues(shardLabel).Set(float64(len(buffer)))
			if len(buffer) >= BatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// ─── Monitoring Routine ───────────────────────────────────────────────────────

func monitoringRoutine(ctx context.Context, shards []chan IntervalRecord) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			processed := atomic.SwapInt64(&msgCounter, 0) / 5
			intervals := atomic.SwapInt64(&intervalCounter, 0) / 5

			// Update channel saturation gauges
			totalCap := float64(ShardCount * ShardChanSize)
			totalUsed := 0.0
			for i, ch := range shards {
				depth := float64(len(ch))
				totalUsed += depth
				shardChannelDepth.WithLabelValues(fmt.Sprintf("%d", i)).Set(depth)
			}
			utilization := totalUsed / totalCap
			channelUtilization.Set(utilization)
			workerQueueDepth.Set(totalUsed) // backwards compat

			// Worker pool saturation
			// NOTE: activeWorkers gauge is maintained by inc/dec in processKafkaStream;
			// here we just log the saturation ratio for the monitoring ticker
			workerPoolSaturation.Set(float64(atomic.LoadInt64(&msgCounter)) / float64(WorkerCount))

			logger.Infof(
				"Throughput: %d msg/s | %d intervals/s | chan_util: %.1f%% | shards: %d",
				processed, intervals, utilization*100, ShardCount,
			)
		}
	}
}

// clickhouseErrors is declared here (outside the var block above) to keep the
// "extra" metrics clearly grouped.
var clickhouseErrors = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "nms_clickhouse_errors_total",
	Help: "ClickHouse errors by shard and operation (prepare/send/timeout). " +
		"Use alongside nms_clickhouse_insert_timeouts_total to distinguish hung vs failed inserts.",
}, []string{"shard", "operation"})