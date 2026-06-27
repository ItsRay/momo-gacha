package main

import (
	"context"
	"database/sql"
	"os"
	"os/signal"
	"syscall"
	"time"

	"momo-gacha/config"
	"momo-gacha/internal/mq"
	"momo-gacha/internal/worker"
	"momo-gacha/pkg/logger"

	_ "github.com/go-sql-driver/mysql"
)

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

	var dbErr error
	for i := 0; i < 10; i++ {
		dbErr = db.Ping()
		if dbErr == nil {
			break
		}
		logger.Warn("Failed to ping MySQL, retrying in 1 second... (%d/10)", i+1)
		time.Sleep(1 * time.Second)
	}
	if dbErr != nil {
		logger.Error("Failed to ping MySQL after 10 attempts: %v", dbErr)
		os.Exit(1)
	}
	logger.Info("Connected to MySQL database successfully.")

	// 3. Initialize Kafka Consumer
	consumer := mq.NewKafkaConsumer(cfg.Kafka.Brokers, cfg.Kafka.Topic, cfg.Kafka.GroupID)
	logger.Info("Kafka Consumer group initialized.")

	// 4. Initialize Worker
	w := worker.NewWorker(cfg, db, consumer)
	defer w.Close()

	// 5. Setup context and graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	workerDone := make(chan struct{})

	go func() {
		// 6. Start Worker Loop in goroutine
		w.Start(ctx)
		close(workerDone)
	}()

	// Block until signal is received
	<-sigChan
	logger.Info("Shutdown signal received. Shutting down worker...")

	// 1. Close worker (e.g. stop Kafka consumer reading new messages)
	if err := w.Close(); err != nil {
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
