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
		Help:    "Time spent processing single Kafka message",
		Buckets: prometheus.DefBuckets,
	})

	// ClickHouse
	clickhouseBatchLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "nms_clickhouse_batch_duration_seconds",
		Help:    "ClickHouse batch insert latency",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
	})

	clickhouseBatchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "nms_clickhouse_batch_size_records",
		Help:    "Number of records per ClickHouse batch",
		Buckets: []float64{100, 500, 1000, 2000, 5000, 10000},
	})

	// Redis
	redisOperationDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "nms_redis_operation_duration_seconds",
		Help:    "Redis operation latency",
		Buckets: prometheus.DefBuckets,
	}, []string{"operation"})

	// Business Metrics
	nodeStateTransitions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "nms_node_state_transitions_total",
		Help: "Node UP/DOWN state transitions",
	}, []string{"node_id", "from_state", "to_state"})

	activeDownNodes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "nms_active_down_nodes",
		Help: "Current number of nodes in DOWN state",
	})

	intervalRecordsCreated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "nms_interval_records_total",
		Help: "Total downtime interval records generated",
	})

	// Health & Queue
	workerQueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "nms_worker_queue_depth",
		Help: "Current depth of interval channel",
	})

	kafkaLagSeconds = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "nms_kafka_consumer_lag_seconds",
		Help: "Kafka consumer lag in seconds",
	}, []string{"topic", "group"})
)

const (
	KafkaBroker   = "localhost:9093"
	KafkaTopic    = "dbserver1.inventory.histalarms"
	KafkaGroup    = "nms-processor-v1"
	RedisAddr     = "localhost:6379"
	ClickHouseAddr = "localhost:9000"

	WorkerCount   = 20
	BatchSize     = 2000
	FlushInterval = 2 * time.Second
)

var (
	msgCounter      int64
	intervalCounter int64
	log             = logrus.New()
)

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
}

func main() {
	log.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Initialize Clients
	rdb := redis.NewClient(&redis.Options{
		Addr:         RedisAddr,
		PoolSize:     100,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	})

	ch, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{ClickHouseAddr},
		Auth: clickhouse.Auth{Database: "default"},
	})
	if err != nil {
		log.Fatalf("ClickHouse Connect Fail: %v", err)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{KafkaBroker},
		Topic:    KafkaTopic,
		GroupID:  KafkaGroup,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	intervalChan := make(chan IntervalRecord, 20000)
	var wg sync.WaitGroup

	log.Infof("NMS Processor started | Workers: %d | BatchSize: %d", WorkerCount, BatchSize)

	// Start Prometheus Metrics Server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Infof("Prometheus metrics available at http://localhost:2112/metrics")
		if err := http.ListenAndServe(":2112", nil); err != nil {
			log.Errorf("Metrics server failed: %v", err)
		}
	}()

	// Start ClickHouse Batcher
	batcherDone := make(chan struct{})
	go runClickHouseBatcher(ctx, ch, intervalChan, batcherDone)

	// Start Workers
	for i := 0; i < WorkerCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			processKafkaStream(ctx, id, reader, rdb, intervalChan)
		}(i)
	}

	// Monitoring
	go monitoringRoutine(ctx)

	<-ctx.Done()
	log.Warn("Shutting down...")
	reader.Close()
	wg.Wait()
	close(intervalChan)
	<-batcherDone
	log.Info("Clean shutdown completed.")
}

func processKafkaStream(ctx context.Context, workerID int, r *kafka.Reader, rdb *redis.Client, out chan<- IntervalRecord) {
	for {
		start := time.Now()

		m, err := r.ReadMessage(ctx)
		if err != nil {
			return
		}

		atomic.AddInt64(&msgCounter, 1)
		kafkaMessageLatency.Observe(time.Since(start).Seconds())

		var env DebeziumPayload
		if err := json.Unmarshal(m.Value, &env); err != nil {
			processingErrors.WithLabelValues("json_unmarshal", "kafka").Inc()
			continue
		}

		data := env.Payload.After
		if data.NodeID == "" {
			continue
		}

		messagesProcessed.WithLabelValues(KafkaTopic, "ok").Inc()

		eventTime := time.UnixMilli(data.EventTime)
		currentStatus := "UP"
		if data.TrapID == 1417 || data.TrapID == 1535 {
			currentStatus = "DOWN"
		}

		key := fmt.Sprintf("node:%s", data.NodeID)

		// Redis Get
		redisStart := time.Now()
		val, err := rdb.Get(ctx, key).Result()
		redisOperationDuration.WithLabelValues("GET").Observe(time.Since(redisStart).Seconds())

		var prevStatus string
		if err == nil {
			var prev NodeState
			if json.Unmarshal([]byte(val), &prev) == nil {
				prevStatus = prev.Status

				if prev.Status == "DOWN" && currentStatus == "UP" {
					dur := eventTime.Sub(prev.LastSeenAt).Seconds()
					if dur > 0 {
						out <- IntervalRecord{
							NodeID:      data.NodeID,
							DownAt:      prev.LastSeenAt,
							UpAt:        eventTime,
							DurationSec: dur,
						}
						atomic.AddInt64(&intervalCounter, 1)
						intervalRecordsCreated.Inc()
						nodeStateTransitions.WithLabelValues(data.NodeID, "DOWN", "UP").Inc()
					}
				}
			}
		}

		// Update Redis
		redisStart = time.Now()
		newState, _ := json.Marshal(NodeState{Status: currentStatus, LastSeenAt: eventTime})
		rdb.Set(ctx, key, newState, 48*time.Hour)
		redisOperationDuration.WithLabelValues("SET").Observe(time.Since(redisStart).Seconds())

		if currentStatus == "DOWN" && prevStatus != "DOWN" {
			nodeStateTransitions.WithLabelValues(data.NodeID, prevStatus, "DOWN").Inc()
		}
	}
}

func runClickHouseBatcher(ctx context.Context, ch clickhouse.Conn, in <-chan IntervalRecord, done chan struct{}) {
	defer close(done)
	buffer := make([]IntervalRecord, 0, BatchSize)
	ticker := time.NewTicker(FlushInterval)

	flush := func() {
		if len(buffer) == 0 {
			return
		}

		start := time.Now()
		batch, err := ch.PrepareBatch(context.Background(), "INSERT INTO node_intervals (node_id, down_at, up_at, duration_sec)")
		if err != nil {
			processingErrors.WithLabelValues("clickhouse", "prepare").Inc()
			return
		}

		for _, rec := range buffer {
			batch.Append(rec.NodeID, rec.DownAt, rec.UpAt, rec.DurationSec)
		}

		if err := batch.Send(); err != nil {
			processingErrors.WithLabelValues("clickhouse", "send").Inc()
		} else {
			clickhouseBatchLatency.Observe(time.Since(start).Seconds())
			clickhouseBatchSize.Observe(float64(len(buffer)))
		}
		buffer = buffer[:0]
	}

	for {
		select {
		case rec, ok := <-in:
			if !ok {
				flush()
				return
			}
			buffer = append(buffer, rec)
			if len(buffer) >= BatchSize {
				flush()
			}
			workerQueueDepth.Set(float64(len(in)))
		case <-ticker.C:
			flush()
		}
	}
}

func monitoringRoutine(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			processed := atomic.SwapInt64(&msgCounter, 0) / 5
			intervals := atomic.SwapInt64(&intervalCounter, 0) / 5
			log.Infof("Throughput: %d msg/sec | %d intervals/sec", processed, intervals)
		}
	}
}