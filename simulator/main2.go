package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
)

const (
	MySQL_DSN         = "root:debezium@tcp(localhost:3307)/inventory?parseTime=true"
	ConcurrentWorkers = 50
	NodeRange         = 15000
	BatchSize         = 1000
	FlushInterval     = 500 * time.Millisecond
	
	// Simulation Settings
	HistoryStart      = 24 * time.Hour // How far back to start generating events
	MaxDownMinutes    = 15             // Max time a node stays down
	MinDownMinutes    = 2              // Min time a node stays down
	MaxUpMinutes      = 240            // Max time a node stays up (4 hours)
	MinUpMinutes      = 30             // Min time a node stays up
)

type Event struct {
	NodeID    string
	TrapID    int
	EventTime int64
}

var (
	totalEvents int64
	deltaEvents int64
	log         = logrus.New()
)

func main() {
	log.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := sql.Open("mysql", MySQL_DSN)
	if err != nil {
		log.Fatalf("Failed to open DB: %v", err)
	}
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(100)

	eventChan := make(chan Event, 50000)
	var wg sync.WaitGroup

	// 1. Start Batch Writer
	wg.Add(1)
	go batchWriter(ctx, &wg, db, eventChan)

	// 2. Start Producers with Virtual Time
	nodesPerWorker := NodeRange / ConcurrentWorkers
	for i := 0; i < ConcurrentWorkers; i++ {
		startNode := i * nodesPerWorker
		endNode := (i + 1) * nodesPerWorker
		wg.Add(1)
		go virtualTimeProducer(ctx, &wg, i, startNode, endNode, eventChan)
	}

	// 3. Monitor
	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ctx.Done(): return
			case <-ticker.C:
				eps := atomic.SwapInt64(&deltaEvents, 0)
				log.Infof("Processing: %d Events/Sec | Total: %d", eps, atomic.LoadInt64(&totalEvents))
			}
		}
	}()

	<-ctx.Done()
	close(eventChan)
	wg.Wait()
	log.Info("Simulation Complete.")
}

func virtualTimeProducer(ctx context.Context, wg *sync.WaitGroup, id, startNode, endNode int, out chan<- Event) {
	defer wg.Done()
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))

	// Each worker tracks the "Current Virtual Time" for its set of nodes
	nodeClocks := make([]time.Time, endNode-startNode)
	startTime := time.Now().Add(-HistoryStart)

	for i := range nodeClocks {
		// Stagger the start times so all nodes don't go down at the exact same minute
		nodeClocks[i] = startTime.Add(time.Duration(rng.Intn(3600)) * time.Second)
	}

	for {
		for i := startNode; i < endNode; i++ {
			select {
			case <-ctx.Done(): return
			default:
				idx := i - startNode
				nodeID := fmt.Sprintf("NODE_%d", i)
				
				// 1. Generate DOWN Event
				out <- Event{
					NodeID:    nodeID,
					TrapID:    1417, // DOWN
					EventTime: nodeClocks[idx].UnixMilli(),
				}

				// Calculate Down Duration (2 to 15 minutes)
				downDuration := time.Duration(rng.Intn(MaxDownMinutes-MinDownMinutes)+MinDownMinutes) * time.Minute
				nodeClocks[idx] = nodeClocks[idx].Add(downDuration)

				// 2. Generate UP Event
				out <- Event{
					NodeID:    nodeID,
					TrapID:    9999, // UP
					EventTime: nodeClocks[idx].UnixMilli(),
				}

				// Calculate Up Duration (30m to 4h)
				upDuration := time.Duration(rng.Intn(MaxUpMinutes-MinUpMinutes)+MinUpMinutes) * time.Minute
				nodeClocks[idx] = nodeClocks[idx].Add(upDuration)

				// If our virtual clock has caught up to real-time, slow down
				if nodeClocks[idx].After(time.Now()) {
					time.Sleep(10 * time.Millisecond)
				}
			}
		}
	}
}

func batchWriter(ctx context.Context, wg *sync.WaitGroup, db *sql.DB, in <-chan Event) {
	defer wg.Done()
	buffer := make([]Event, 0, BatchSize)
	ticker := time.NewTicker(FlushInterval)

	flush := func() {
		if len(buffer) == 0 { return }
		
		valStrs := make([]string, 0, len(buffer))
		valArgs := make([]interface{}, 0, len(buffer)*3)
		for _, e := range buffer {
			valStrs = append(valStrs, "(?, ?, ?)")
			valArgs = append(valArgs, e.NodeID, e.TrapID, e.EventTime)
		}

		query := fmt.Sprintf("INSERT INTO histalarms (node_id, trap_id, event_time) VALUES %s", strings.Join(valStrs, ","))
		_, err := db.Exec(query, valArgs...)
		if err != nil {
			log.Errorf("DB Error: %v", err)
		} else {
			atomic.AddInt64(&totalEvents, int64(len(buffer)))
			atomic.AddInt64(&deltaEvents, int64(len(buffer)))
		}
		buffer = buffer[:0]
	}

	for {
		select {
		case e, ok := <-in:
			if !ok { flush(); return }
			buffer = append(buffer, e)
			if len(buffer) >= BatchSize { flush() }
		case <-ticker.C:
			flush()
		}
	}
}