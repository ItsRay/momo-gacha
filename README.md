# momo-gacha

本專案是一個 **模擬 momo 購物網行銷活動（Campaign / Gacha 抽獎系統）** 的後端服務，專為高併發、限量商品抽獎以及非同步批量落庫等情境設計。

本專案附有以下核心設計文件可供參閱：
* 📋 **[PRD.md](file:///Users/ray/GolandProjects/momo-gacha/PRD.md)** - 產品需求與業務規則定義（包含中獎概率、保底機制與 API 規格）。
* 📐 **[TDD.md](file:///Users/ray/GolandProjects/momo-gacha/TDD.md)** - 技術架構與設計文件（包含兩層式庫存扣減防超賣、Kafka 非同步批量落庫與防連點冪等性設計）。

---

## 🚀 快速啟動與基準測試 (Quick Start & Benchmark)

本專案的所有服務與基礎設施（API Server, Worker, Redis, MySQL, Kafka）已完全 Docker 化，並配有 **`Makefile`**。**本機無需安裝 Go 環境，即可直接複製下方指令完成啟動與 E2E 驗證**。

### ⚡ 3秒極速複製貼上測試 (TL;DR)
```bash
# 1. 一鍵啟動所有 Docker 容器（API Server, Worker, Redis, MySQL, Kafka）
make compose-up

# 2. 運行高併發整合驗證腳本（模擬 200 位用戶搶抽大獎、連點阻擋與非同步落庫驗收）
make benchmark

# 3. 測試完畢後，一鍵清理容器與暫存資料
make compose-down
```
> [!TIP]
> 您亦可直接於專案根目錄輸入 `make` 或 `make help` 檢視所有可用指令說明。

---

### 📖 步驟詳細說明與驗證細節

#### 1. 啟動所有容器服務 (`make compose-up`)
這將自動拉起所有容器服務。MySQL 啟動時會透過 `scripts/schema.sql` 自動初始化資料庫表結構。API 伺服器會監聽在本地 `http://localhost:8080`。

#### 2. 執行 E2E 併發與自癒基準驗證 (`make benchmark`)
此指令會自動在 Docker 內部拉起一個一次性的 Golang 容器，直接透過容器內網（`http://api:8080`）對 API 服務進行高併發抽獎與功能驗證：
1. **活動初始化**：自動呼叫 Admin API 建立一個新抽獎活動，限量大獎庫存為 5、二獎限量庫存為 20、以及保底獎（銘謝惠顧）。
2. **高併發搶抽**：模擬 200 個併發請求搶抽該限量大獎，驗證高併發下庫存是否會超賣，以及大獎售罄後是否自動降級為保底獎項。
3. **冪等性防連點**：同時發送重複的 `Idempotency-Key` 請求，驗證功能是否精準防範重複扣減與連點。
4. **非同步批量落庫驗收**：最後讀取 Admin 統計數據，確認資料庫是否經由 Background Worker 批次非同步安全落庫且資料最終一致。
5. **產生報告**：測試結束後會自動在專案根目錄產生測試報告檔 `benchmark_report.txt` 可供查閱。

#### 3. 清理容器與暫存資料 (`make compose-down`)
停止所有運行的容器，並清理暫存的資料庫與快取資料。

---

## 🧪 執行單元測試 (Unit Test)

若您本地已有 Go 環境，可以執行以下命令驗證核心隨機演算法與補償交易機制：
```bash
make test
```

---

## 📂 專案結構 (Project Structure)

```
momo-gacha/
├── cmd/
│   ├── api/
│   │   └── main.go               # API Server 進入點 (負責接聽 HTTP Request)
│   └── worker/
│       └── main.go               # Background Worker 進入點 (負責消化 Queue 處理觸達)
├── internal/
│   ├── handler/                  # HTTP 介接層 (負責解析 Request、驗證參數、呼叫 Usecase)
│   │   ├── admin_handler.go      # 管理端 API 接口 (活動建立/更新/統計)
│   │   └── gacha_handler.go      # 用戶端 API 接口 (執行抽獎)
│   ├── usecase/                  # [核心心臟] 業務情境層 (負責機率運算、降級邏輯)
│   │   ├── campaign_usecase.go   # 管理活動與獎品權重
│   │   └── draw_gacha_usecase.go # 核心抽獎引擎 (Layer 1 + Layer 2 庫存扣減)
│   ├── repository/               # 資料存取層 (負責與外部組件如 Redis, DB 溝通)
│   │   ├── mysql/
│   │   │   └── db_repo.go        # 實作 MySQL 批次落庫與樂觀鎖更新
│   │   ├── redis/
│   │   │   └── gacha_repo.go     # 實作 Redis 配置讀寫、Pipeline 批次庫存查詢
│   │   └── lua/
│   │       ├── deduct_stock.lua  # 防超賣原子性扣減 Lua Script
│   │       └── lua.go            # 靜態嵌入 Lua 腳本工具
│   ├── mq/                       # Message Queue 層 (負責發布/訂閱非同步事件)
│   │   ├── publisher.go          # Kafka 事件發布實作 (帶有 3 次重試退避)
│   │   └── consumer.go           # Kafka 事件消費實作 (帶有雙條件 batch 合併落庫與 DLQ 隔離)
│   └── domain/                   # 領域層 (存放跨層共用的 Structs, Event Payload 與 Interfaces)
│       ├── campaign.go           #活動與獎品領域模型、Repository 介面宣告
│       └── event.go              # 中獎事件領域模型、Publisher/Consumer 介面宣告
├── pkg/                          # 共用工具包 (如 Logger, Error Handling)
│   ├── logger/                   # 簡明日誌輸出工具
│   └── response/                 # 統一 HTTP 響應格式工具
├── config/                       # 設定檔
│   └── config.yaml               # 本地/環境預設配置
├── scripts/                      # 腳本目錄
│   ├── schema.sql                # MySQL 表結構定義檔案
│   └── benchmark.go              # 端到端高併發壓力模擬腳本
├── Dockerfile.api                # API Server 的 Dockerfile
├── Dockerfile.worker             # Background Worker 的 Dockerfile
├── docker-compose.yml            # 基礎設施與服務一鍵 Docker-Compose 部署配置
├── Makefile                      # 專案快捷指令定義檔
├── PRD.md                        # 產品需求文件
├── TDD.md                        # 技術設計文件
├── README.md                     # 專案說明與啟動指南
├── go.mod
└── go.sum
```
