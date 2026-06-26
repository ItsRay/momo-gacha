package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"time"

	"momo-gacha/config"
	"momo-gacha/internal/handler"
	"momo-gacha/internal/mq"
	redisRepo "momo-gacha/internal/repository/redis"
	"momo-gacha/internal/usecase"
	"momo-gacha/pkg/logger"

	_ "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
)

func main() {
	logger.Info("Starting momo-gacha API Server...")

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

	// 3. Connect to Redis Client
	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.Redis.Addr,
		Password:     cfg.Redis.Password,
		DB:           cfg.Redis.DB,
		PoolSize:     500,               // 增大連線池以承載高頻抽獎請求
		MinIdleConns: 50,                // 保持最小空閒連線，防止冷啟動延遲
	})
	defer rdb.Close()

	// Quick check Redis connection
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		logger.Error("Failed to connect to Redis: %v", err)
		os.Exit(1)
	}
	logger.Info("Connected to Redis successfully.")

	// 4. Initialize Repositories
	campaignRepo := redisRepo.NewGachaRepository(db, rdb)
	// (rewardRepo is not used on API side, only on Worker side, but we initialize campaignRepo here)

	// 5. Initialize MQ Publisher
	publisher := mq.NewKafkaPublisher(cfg.Kafka.Brokers, cfg.Kafka.Topic)
	defer publisher.Close()
	logger.Info("Kafka Publisher initialized.")

	// 6. Initialize Usecases
	campaignUC := usecase.NewCampaignUsecase(campaignRepo)
	drawGachaUC := usecase.NewDrawGachaUsecase(campaignRepo, publisher, rdb)

	// 7. Initialize Handlers
	adminHandler := handler.NewAdminHandler(campaignUC)
	gachaHandler := handler.NewGachaHandler(drawGachaUC)

	// 8. Register Routes (Go 1.22+ ServeMux patterns)
	mux := http.NewServeMux()

	// Admin APIs
	mux.HandleFunc("POST /v1/admin/gachas", adminHandler.CreateCampaign)
	mux.HandleFunc("PUT /v1/admin/gachas/{id}/prizes", adminHandler.UpdatePrizeWeights)
	mux.HandleFunc("GET /v1/admin/gachas/{id}/stats", adminHandler.GetCampaignStats)

	// Client APIs
	mux.HandleFunc("POST /v1/gachas/{id}/draw", gachaHandler.Draw)

	// 9. Listen and Serve
	addr := fmt.Sprintf(":%d", cfg.Server.Port)
	logger.Info("API Server listening on %s...", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		logger.Error("API Server crashed: %v", err)
		os.Exit(1)
	}
}
