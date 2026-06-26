# momo-gacha

momo 面試的作業 (Gacha System).

## 專案結構 (Project Structure)

```
momo-gacha/
├── cmd/
│   ├── api/
│   │   └── main.go               # API Server 進入點 (負責接聽 HTTP Request)
│   └── worker/
│       └── main.go               # Background Worker 進入點 (負責消化 Queue 處理觸達)
├── internal/
│   ├── handler/                  # HTTP 介接層 (負責解析 Request、驗證參數、呼叫 Usecase)
│   │   ├── admin_handler.go
│   │   └── gacha_handler.go
│   ├── usecase/                  # [核心心臟] 業務情境層 (負責機率運算、降級邏輯)
│   │   ├── draw_gacha_usecase.go # 包含第一層權重計算與第二層 Redis 呼叫
│   │   └── update_prize_usecase.go
│   ├── repository/               # 資料存取層 (負責與外部組件如 Redis, DB 溝通)
│   │   ├── redis/
│   │   │   └── gacha_repo.go     # 實作 Redis 讀寫邏輯
│   │   └── lua/
│   │       └── deduct_stock.lua  # 防超賣原子性扣減 Script
│   ├── mq/                       # Message Queue 層 (負責發布/訂閱非同步事件)
│   │   ├── publisher.go
│   │   └── consumer.go
│   └── domain/                   # 領域層 (存放跨層共用的 Structs, Event Payload 與 Interfaces)
│       ├── campaign.go
│       └── event.go
├── pkg/                          # 共用工具包 (如 Logger, Error Handling)
│   ├── logger/
│   └── response/
├── config/                       # 設定檔
│   └── config.yaml               # 或使用 .env
├── PRD.md                        # 產品需求文件
├── TDD.md                        # 技術設計文件
├── README.md                     # 專案說明與啟動指南
├── go.mod
└── go.sum
```

---

## 快速啟動指引 (Quick Start)

### 1. 安裝與執行環境需求
- Go 1.20+
- Redis (大獎庫存與 MQ 管道)

### 2. 啟動服務
詳細的啟動步驟與 API 驗證測試將於專案開發完成後在此列出。
- 啟動 API Server: `go run cmd/api/main.go`
- 啟動 Background Worker: `go run cmd/worker/main.go`

---

## 未來展望 (Future Roadmap)

* **引進 GORM (ORM)**：提升 Admin CRUD 開發速度與預設防範 SQL 注入；未來規劃採「Admin CRUD 使用 GORM」與「核心高併發/Worker 使用原生 SQL」的混合架構。
* **可觀測性監控 (Observability)**：導入 OpenTelemetry 鏈路追蹤與 Prometheus + Grafana 指標監控。
