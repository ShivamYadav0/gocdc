package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"
"github.com/prometheus/client_golang/prometheus/promauto"
"github.com/prometheus/client_golang/prometheus/promhttp"
"net/http"
)
var (
    opsProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "nms_processor_messages_total",
        Help: "The total number of Kafka messages processed",
    }, []string{"topic", "status"})

    processingErrors = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "nms_processor_errors_total",
        Help: "Processing errors",
    }, []string{"type"})
	clickhouseBatchLatency = promauto.NewHistogram(prometheus.HistogramOpts{
        Name:    "nms_clickhouse_batch_duration_seconds",
        Help:    "Latency of ClickHouse batch inserts",
        Buckets: []float64{.05, .1, .5, 1, 2, 5},
    })
)

const (
	KafkaBroker      = "localhost:9093"
	KafkaTopic       = "dbserver1.inventory.histalarms"
	KafkaGroup       = "nms-processor-v1"
	RedisAddr        = "localhost:6379"
	ClickHouseAddr   = "localhost:9000"
	
	WorkerCount      = 20    // Parallel workers for Redis logic
	BatchSize        = 2000  // Optimal for ClickHouse ingestion
	FlushInterval    = 2 * time.Second
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

	// 1. Initialize Clients
	rdb := redis.NewClient(&redis.Options{
		Addr:         RedisAddr,
		PoolSize:     50, // Allow high concurrency
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})

	ch, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{ClickHouseAddr},
		Auth: clickhouse.Auth{Database: "default"},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
	})
	if err != nil {
		log.Fatalf("ClickHouse Connect Fail: %v", err)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{KafkaBroker},
		Topic:    KafkaTopic,
		GroupID:  KafkaGroup,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	// 2. Channels for Pipeline
	intervalChan := make(chan IntervalRecord, 10000)
	var wg sync.WaitGroup

	// 3. Start Components
	log.Infof("Processor initialized. Workers: %d | BatchSize: %d", WorkerCount, BatchSize)

	// Start ClickHouse Batcher
	batcherDone := make(chan struct{})
	go runClickHouseBatcher(ctx, ch, intervalChan, batcherDone)


	go func() {
        http.Handle("/metrics", promhttp.Handler())
        log.Infof("Starting Prometheus metrics server on :2112")
        if err := http.ListenAndServe(":2112", nil); err != nil {
            log.Errorf("Metrics server failed: %v", err)
        }
    }()

	// Start Workers
	for i := 0; i < WorkerCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			processKafkaStream(ctx, id, reader, rdb, intervalChan)
		}(i)
	}

	// 4. Monitoring Goroutine
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				processed := atomic.SwapInt64(&msgCounter, 0) / 5
				intervals := atomic.SwapInt64(&intervalCounter, 0) / 5
				log.Infof("Statistics: %d Messages/sec | %d Intervals/sec", processed, intervals)
			}
		}
	}()

	// 5. Graceful Shutdown
	<-ctx.Done()
	log.Warn("Shutting down... draining workers and buffers.")
	reader.Close()
	wg.Wait()
	close(intervalChan)
	<-batcherDone
	log.Info("Processor exit clean.")
}

func processKafkaStream(ctx context.Context, workerID int, r *kafka.Reader, rdb *redis.Client, out chan<- IntervalRecord) {
	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			return // Context cancelled or reader closed
		}
		//log.Infof("Worker %d received raw message: %s", workerID, string(m.Value))
		atomic.AddInt64(&msgCounter, 1)
		

		var env DebeziumPayload
		if err := json.Unmarshal(m.Value, &env); err != nil {
			continue
		}

		data := env.Payload.After
		if data.NodeID == "" {
			continue
		}
		opsProcessed.WithLabelValues(data.NodeID, "ok").Inc()

		// Core Logic: State Machine
		eventTime := time.UnixMilli(data.EventTime)
		currentStatus := "UP"
		if data.TrapID == 1417 || data.TrapID == 1535 {
			currentStatus = "DOWN"
		}

		key := fmt.Sprintf("node:%s", data.NodeID)
		
		// 1. Check previous state in Redis
		val, err := rdb.Get(ctx, key).Result()
		if err == nil {
			var prev NodeState
			if err := json.Unmarshal([]byte(val), &prev); err == nil {
				processingErrors.WithLabelValues("json_unmarshal").Inc()
				// Transition: If previous was DOWN and current is UP, calculate interval
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
					}
				}
			}
		}

		// 2. Update current state in Redis
		newState, _ := json.Marshal(NodeState{Status: currentStatus, LastSeenAt: eventTime})
		rdb.Set(ctx, key, newState, 48*time.Hour)
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

		batch, err := ch.PrepareBatch(context.Background(), "INSERT INTO node_intervals")
		if err != nil {
			log.Errorf("ClickHouse Prepare Error: %v", err)
			return
		}

		for _, rec := range buffer {
			if err := batch.Append(rec.NodeID, rec.DownAt, rec.UpAt, rec.DurationSec); err != nil {
				log.Errorf("Append Error: %v", err)
			}
		}

		if err := batch.Send(); err != nil {
			log.Errorf("Batch Send Error: %v", err)
		} else {
			log.Infof("Committed %d intervals to ClickHouse", len(buffer))
		}
		buffer = buffer[:0]
	}

	for {
		select {
		case rec, ok := <-in:
			if !ok {
				flush() // Final flush on shutdown
				return
			}
			buffer = append(buffer, rec)
			if len(buffer) >= BatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}