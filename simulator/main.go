package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"math/rand"
)

const (
	MySQL_DSN         = "root:debezium@tcp(localhost:3307)/inventory?parseTime=true"
	ConcurrentWorkers = 50     // Increased for higher load
	NodeRange         = 15000  // Match your 15k nodes requirement
	BatchSize         = 500   // How many rows per single SQL statement
	FlushInterval     = 1 * time.Second
	ChannelBufferSize = 10000
)

type Event struct {
	NodeID    string
	TrapID    int
	EventTime int64
}

var (
	totalEvents  int64
	deltaEvents  int64 // For calculating EPS
	log          = logrus.New()
)

func main() {
	log.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 1. Database Tuning
	db, err := sql.Open("mysql", MySQL_DSN)
	if err != nil {
		log.Fatalf("Failed to open DB: %v", err)
	}
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(100) // Keep connections alive for performance
	db.SetConnMaxLifetime(10 * time.Minute)

	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("MySQL Unreachable: %v", err)
	}

	// 2. High-Speed Event Channel
	eventChan := make(chan Event, ChannelBufferSize)
	var wg sync.WaitGroup

	// 3. Start the Batch Writer (The Performance Engine)
	wg.Add(1)
	go batchWriter(ctx, &wg, db, eventChan)

	// 4. Start Simulated Workers (The Data Producers)
	for i := 1; i <= ConcurrentWorkers; i++ {
		wg.Add(1)
		go producer(ctx, &wg, i, eventChan)
	}

	// 5. Throughput Monitor (EPS Meter)
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				eps := atomic.SwapInt64(&deltaEvents, 0)
				total := atomic.LoadInt64(&totalEvents)
				log.Infof("Throughput: %d EPS | Total Events: %d | Workers: %d", eps, total, ConcurrentWorkers)
			}
		}
	}()

	<-ctx.Done()
	log.Warn("Shutdown signal received. Cleaning up...")
	close(eventChan)
	wg.Wait()
	db.Close()
	log.Info("Simulator stopped cleanly.")
}

// producer generates random UP/DOWN transitions
func producer(ctx context.Context, wg *sync.WaitGroup, id int, out chan<- Event) {
	defer wg.Done()
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))

	for {
		select {
		case <-ctx.Done():
			return
		default:
			nodeID := fmt.Sprintf("NODE_%d", rng.Intn(NodeRange))

			// Generate DOWN event
			out <- Event{NodeID: nodeID, TrapID: 1417, EventTime: time.Now().UnixMilli()}

			// Sleep for "downtime" (shorter for speed simulation)
			time.Sleep(time.Duration(rng.Intn(1000)+500) * time.Millisecond)

			// Generate UP event
			out <- Event{NodeID: nodeID, TrapID: 9999, EventTime: time.Now().UnixMilli()}

			// Faster cycle
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// batchWriter collects events and executes a single multi-row INSERT
func batchWriter(ctx context.Context, wg *sync.WaitGroup, db *sql.DB, in <-chan Event) {
	defer wg.Done()
	buffer := make([]Event, 0, BatchSize)
	ticker := time.NewTicker(FlushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(buffer) == 0 {
			return
		}
		
		// Create the multi-row insert query: INSERT INTO ... VALUES (?,?,?), (?,?,?) ...
		valueStrings := make([]string, 0, len(buffer))
		valueArgs := make([]interface{}, 0, len(buffer)*3)

		for _, e := range buffer {
			valueStrings = append(valueStrings, "(?, ?, ?)")
			valueArgs = append(valueArgs, e.NodeID, e.TrapID, e.EventTime)
		}

		query := fmt.Sprintf("INSERT INTO histalarms (node_id, trap_id, event_time) VALUES %s", 
			strings.Join(valueStrings, ","))

		_, err := db.ExecContext(context.Background(), query, valueArgs...)
		if err != nil {
			log.Errorf("Batch Insert Error: %v", err)
		} else {
			count := int64(len(buffer))
			atomic.AddInt64(&totalEvents, count)
			atomic.AddInt64(&deltaEvents, count)
		}
		buffer = buffer[:0] // Reset buffer
	}

	for {
		select {
		case event, ok := <-in:
			if !ok {
				flush()
				return
			}
			buffer = append(buffer, event)
			if len(buffer) >= BatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}