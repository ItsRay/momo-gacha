package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"momo-gacha/config"
	"momo-gacha/internal/domain"
	"momo-gacha/internal/mq"
	mysqlRepo "momo-gacha/internal/repository/mysql"
	"momo-gacha/pkg/logger"

	_ "github.com/go-sql-driver/mysql"
	"github.com/segmentio/kafka-go"
)

type Worker struct {
	cfg        *config.Config
	rewardRepo domain.RewardRepository
	consumer   domain.MessageConsumer
	dlqWriter  *kafka.Writer
}

func NewWorker(cfg *config.Config, db *sql.DB, consumer domain.MessageConsumer) *Worker {
	// Initialize DLQ Kafka writer directly
	dlqWriter := &kafka.Writer{
		Addr:     kafka.TCP(cfg.Kafka.Brokers...),
		Topic:    cfg.Kafka.DLQTopic,
		Balancer: &kafka.LeastBytes{},
	}

	return &Worker{
		cfg:        cfg,
		rewardRepo: mysqlRepo.NewRewardRepository(db),
		consumer:   consumer,
		dlqWriter:  dlqWriter,
	}
}

func (w *Worker) Start(ctx context.Context) {
	logger.Info("Starting momo-gacha Reach Worker...")

	// Listen to events using batch consumer
	// BatchSize = 500, Timeout = 200ms
	err := w.consumer.ConsumeEvents(ctx, 500, 200*time.Millisecond, w.handleBatch)
	if err != nil {
		if ctx.Err() == nil {
			logger.Error("Consumer loop crashed: %v", err)
		}
	}
}

func (w *Worker) handleBatch(ctx context.Context, events []domain.RewardEvent) error {
	logger.Info("Received a batch of %d events from Kafka. Processing...", len(events))

	// 1. Prepare records and aggregate stock deductions
	records := make([]domain.RewardRecord, len(events))
	stockDeductions := make(map[string]int)

	for i, ev := range events {
		records[i] = domain.RewardRecord{
			ID:         ev.EventID,
			CampaignID: ev.CampaignID,
			UserID:     ev.UserID,
			PrizeID:    ev.PrizeID,
			CreatedAt:  time.Unix(ev.Timestamp, 0),
		}
		stockDeductions[ev.PrizeID]++
	}

	// 2. Try Batch persistence
	err := w.rewardRepo.ExecuteBatchTransaction(ctx, records, stockDeductions)
	if err == nil {
		logger.Info("Successfully committed batch of %d records to database.", len(records))
		// Execute simulated reach actions
		for _, ev := range events {
			w.triggerReachSimulation(ev)
		}
		return nil
	}

	// 3. Batch Failed: Fallback to Sequential processing
	logger.Warn("Batch execution failed: %v. Falling back to sequential processing...", err)

	for i, rec := range records {
		ev := events[i]
		// Process each item in a single database transaction
		// Deduct 1 stock and insert 1 record
		singleErr := w.processSingleRecord(ctx, rec)
		if singleErr != nil {
			logger.Error("Sequential processing failed for event %s (User: %s, Prize: %s): %v. Sending to DLQ...",
				rec.ID, rec.UserID, rec.PrizeID, singleErr)

			// Forward to DLQ
			w.sendToDLQ(ctx, ev, singleErr.Error())
		} else {
			// Single processing succeeded, trigger reach
			w.triggerReachSimulation(ev)
		}
	}

	// Return nil so that the consumer commits the batch offsets
	// Since all events in this batch are now either written or isolated in DLQ
	return nil
}

func (w *Worker) processSingleRecord(ctx context.Context, rec domain.RewardRecord) error {
	return w.rewardRepo.ExecuteSingleTransaction(ctx, rec)
}

func (w *Worker) triggerReachSimulation(ev domain.RewardEvent) {
	// Mock Push Notification
	fmt.Printf("[REACH][PUSH] 已發送通知給 %-10s：恭喜抽中 %s！\n", ev.UserID, ev.PrizeName)
	// Mock Asset Dispatch
	fmt.Printf("[REACH][ASSET] 已派發資產給 %-10s (獎品 ID: %s)\n", ev.UserID, ev.PrizeID)
}

type DLQMessage struct {
	Event     domain.RewardEvent `json:"event"`
	ErrorMsg  string             `json:"error_msg"`
	FailTime  time.Time          `json:"fail_time"`
}

func (w *Worker) sendToDLQ(ctx context.Context, ev domain.RewardEvent, errMsg string) {
	dlqMsg := DLQMessage{
		Event:    ev,
		ErrorMsg: errMsg,
		FailTime: time.Now(),
	}

	data, err := json.Marshal(dlqMsg)
	if err != nil {
		logger.Error("Failed to serialize DLQ message: %v", err)
		return
	}

	err = w.dlqWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(ev.UserID),
		Value: data,
	})
	if err != nil {
		logger.Error("CRITICAL: Failed to publish message to Kafka DLQ: %v", err)
	} else {
		logger.Info("Event %s successfully isolated in DLQ.", ev.EventID)
	}
}

func (w *Worker) Close() error {
	_ = w.consumer.Close()
	return w.dlqWriter.Close()
}

func main() {
	logger.Info("Initializing Reach Worker Service...")

	// 1. Load config
	cfg, err := config.LoadConfig("config/config.yaml")
	if err != nil {
		logger.Error("Failed to load config: %v", err)
		os.Exit(1)
	}

	// 2. Connect to MySQL Database
	db, err := sql.Open("mysql", cfg.Database.DSN)
	if err != nil {
		logger.Error("Failed to connect to MySQL: %v", err)
		os.Exit(1)
	}
	defer db.Close()

	// Configure MySQL connection pool (防爆連線上限、提升複用率)
	db.SetMaxOpenConns(100)              // 限制最大連線數，防止高併發打爆 MySQL max_connections
	db.SetMaxIdleConns(50)               // 保持空閒連線，免除連線建立的 TCP 握手損耗
	db.SetConnMaxLifetime(1 * time.Hour) // 設定連線生命週期，避免連線洩漏

	if err := db.Ping(); err != nil {
		logger.Error("Failed to ping MySQL: %v", err)
		os.Exit(1)
	}
	logger.Info("Connected to MySQL database successfully.")

	// 3. Initialize Kafka Consumer
	consumer := mq.NewKafkaConsumer(cfg.Kafka.Brokers, cfg.Kafka.Topic, cfg.Kafka.GroupID)
	logger.Info("Kafka Consumer group initialized.")

	// 4. Initialize Worker
	worker := NewWorker(cfg, db, consumer)
	defer worker.Close()

	// 5. Setup context and graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	workerDone := make(chan struct{})

	go func() {
		// 6. Start Worker Loop in goroutine
		worker.Start(ctx)
		close(workerDone)
	}()

	// Block until signal is received
	<-sigChan
	logger.Info("Shutdown signal received. Shutting down worker...")

	// 1. Close worker (e.g. stop Kafka consumer reading new messages)
	if err := worker.Close(); err != nil {
		logger.Error("Failed to close worker: %v", err)
	}

	// 2. Wait for worker to finish processing the current batch (with 10s timeout)
	select {
	case <-workerDone:
		logger.Info("Worker stopped gracefully.")
	case <-time.After(10 * time.Second):
		logger.Warn("Graceful shutdown timeout exceeded. Forcing cancellation...")
		cancel() // Force cancel the context to unblock remaining DB/MQ processes
		<-workerDone
	}
}
