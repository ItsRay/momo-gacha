# 🛠️ 【momo 品牌狂歡】整點幸運轉盤系統 - 技術設計文件 (TDD)

## 1. 系統架構總覽與微服務交互 (High-Level Architecture)

本系統採用 Go 語言開發，在架構上拆分為負責處理前端請求的 `Gacha API Service`，與負責非同步任務的 `Reach Worker Service`。系統整體由兩大核心動線組成：

### 🔄 動線 A：營運管理動線 (Admin Flow)

* **活動建立/更新**：營運端呼叫 Admin API 建立轉盤活動。
* **持久化與快取寫入**：系統將活動元數據寫入關聯式資料庫（MySQL），並將「機率權重與初始庫存」寫入 Redis 快取中。

### 🎰 動線 B：用戶抽獎與事件驅動動線 (Event-Driven Flow)

* **即時開獎 (同步)**：用戶呼叫 `Gacha API Service`。系統在 API 內部即時完成運算與 Redis 庫存原子性扣減，當場回傳結果給用戶。
* **事件發布 (非同步)**：API 確定結果後，扮演 Producer 將中獎事件推送至 Kafka。
* **統一寫入與觸達 (非同步 Consumer)**：`Reach Worker Service` 訂閱該 Topic，統一處理所有耗時 I/O，包含「中獎明細批次落庫 MySQL」與「呼叫下游發送資產/推播」。

## 2. 系統承載量評估 & 可行性評估

為因應 618 檔期的 10,000 QPS 瞬間寫入流量，本系統進行了以下可行性分析與架構估算：

| 指標 | PRD 要求 | 本架構實作目標 | 滿足說明 |
| :--- | :--- | :--- | :--- |
| **峰值 QPS** | 10,000 QPS | 10,000+ QPS | 透過 K8s HPA 水平擴展 API Pods 消化流量 |
| **回應時間** | 1s - 2s | P99 < 50ms | 僅執行 Redis 原子扣減，實現極速回應 |
| **可用性** | 99.99% | 99.99% | 透過斷路器與 Master-Replica 確保高可用 |

* **Redis 快取層**：Redis 基於全記憶體操作且採 I/O 多路復用，依據 Redis 官方基準指標與一般生產環境預期承載量，單節點執行此類輕量 Lua 腳本（無複雜循環）預期可承載數萬 QPS，足以應對 10,000 QPS 的瞬間流量。
* **Kafka 消息佇列**：Kafka 單分區順序寫入吞吐量可達數萬 TPS，API 端推送無效能瓶頸。
* **MySQL 資料庫層 (壓降 99.8%)**：MySQL 寫入極限為 1k-3k TPS。系統透過 **Kafka 削峰** 與 **Worker 批量落庫**（`BatchSize = 500`），將 10,000 筆寫入合併壓降為：
  $$\text{資料庫每秒交易數 (TPS)} = \frac{10,000}{500} = 20 \text{ TPS}$$
  以每秒僅 20 次批次交易執行 Bulk Write，徹底消除 DB 崩潰風險。


## 3. 核心技術方案：雙層抽獎引擎 (Core Engine)

為實作 PRD 要求的「兜底降級機制」且維持極致效能，抽獎邏輯拆分為兩層，統一於 `Gacha API Service` 執行：

* **第一層（記憶體權重運算）**：從 Redis 取得獎池基礎配置，自動補滿 `type: fallback` 的兜底機率，利用 Weighted Random 演算法判定初步結果。
* **第二層（Redis 庫存扣減與優雅降級）**：若判定抽中「限量獎項」，攜帶獎品 ID 呼叫 Redis 執行 **Lua Script** 扣減庫存。
  * **扣減成功**：Lua 驗證庫存大於 0 並扣減，回傳 `1`，確立中大獎。
  * **庫存耗盡 (Fallback)**：Lua 發現庫存為 0，回傳 `-2`（工程錯誤碼）。API 層攔截此狀態，不拋出錯誤，而是 **默默將結果置換為「兜底獎項 ID」**，達成平滑防超賣。

## 4. 統一高可用架構與容錯策略 (HA & Fault Tolerance)

### 4.1 資料狀態分離與快取重建 (Cache Hydration)

* **活動設定 (機率與規則)**：以 MySQL 為 Source of Truth。若 Cache Miss，使用 `singleflight` 查詢 DB 並重建，防止快取擊穿。
* **剩餘庫存 (高頻狀態)**：以 Redis 為 Source of Truth。若 Redis 庫存 Key 失效，攔截 Lua 返回的 `-1` 狀態，利用 `singleflight` 自 MySQL 讀取並 `SET` 重建，避免無故降級。

### 4.2 Kafka 消費與 MySQL 落庫管線 (Consume & Write Pipeline)

1. **雙條件批次拉取 (Batching)**：達標 `BatchSize = 500` 或 `Timeout = 200ms` 即批次處理，依 `prize_id` 在記憶體彙總，合併 SQL 扣減。
2. **批次交易執行 (Batch Transaction)**：使用 DB Transaction 執行 `Bulk Insert records` 與彙總扣減。成功後 Commit 並 ACK Kafka。
3. **異常降級與精準隔離 (Batch-to-Single Fallback)**：當彙總扣減失敗，Rollback 並退化為**單筆 Transaction 執行**。成功者 Commit，異常者（如超賣）隔離至 **DLQ**，確保正常訊息不被連坐。
4. **優雅關閉 (Graceful Shutdown)**：收到關機訊號先 Close Kafka Consumer 停止拉取，等記憶體緩衝區批次完全落庫 Commit 後，最後才 Close DB。

### 4.3 業務補償與斷路器防護 (Fallback & Fail-Fast)

* **業務補償**：DLQ 異常訂單（如超賣）觸發自動補償（如派發等值點數與致歉）。
* **防重抽獎與 RTT 優化**：
  * 使用 `Idempotency-Key`，改為**直接 `SETNX` 搶鎖**。
  * **成功 (99.9% 正常流量)**：直接抽獎與 Lua 扣庫存，Redis RTT 減少 50%。
  * **失敗 (連點/重試)**：`GET` 查詢鎖狀態，`processing` 則回傳 HTTP 409；已完成則直接返回快取的中獎結果。
* **快速失敗 (Fail-Fast)**：Redis 癱瘓時觸發熔斷 (Circuit Breaker) 直接回傳 HTTP 503，保護底層。

### 4.4 非同步觸達發送與通知模擬 (Reach Notification Simulation)

為了落實抽獎核心與通知發送的併發解耦，本系統採用非同步事件觸達設計：

* **非同步觸達路由**：API 同步開獎成功後，發送事件至 Kafka。`Reach Worker` 作為 Consumer 訂閱該 Topic，並在資料庫記錄成功提交後，直接調用 `triggerReachSimulation` 執行 App 推播與資產發行模擬（以主控台日誌形式輸出）。
* **容錯隔離與 DLQ**：若在資料庫寫入或處理過程中發生異常（如資料庫庫存不足或資料衝突），該事件將退化為單筆事務處理，最終仍失敗者會被隔離發送至死信隊列（DLQ）中，以保證整個通知管線高可用且不堵塞。

## 5. 資料模型與基礎設施設計 (Data Model)

### 5.1 MySQL 關聯式模型 (Source of Truth)

採用 MySQL 為底層儲存，依賴其 ACID 特性與行級鎖 (Row-Level Lock)，作為財務對帳與庫存樂觀鎖的最終防線：

**gacha\_campaigns (活動主表)**
| 欄位 | 說明 |
| :--- | :--- |
| `id` | PK |
| `name` | 活動名稱 |
| `status` | 狀態 (draft / active / ended) |

**gacha\_prizes (獎項配置表)**
| 欄位 | 說明 |
| :--- | :--- |
| `id` | PK |
| `gacha_campaign_id` | 關聯活動 FK |
| `type` | 獎項類型 (limited 限量 / fallback 兜底) |
| `prob_bps` | 命中機率權重 (萬分比，10000 = 100%) |
| `init_stock` | 初始發行總量 |
| `remained_stock` | 剩餘可抽庫存 (Worker 依此執行 `UPDATE ... WHERE remained_stock >= ?` 樂觀鎖) |

**gacha\_reward\_records (中獎明細軌跡表)**
| 欄位 | 說明 |
| :--- | :--- |
| `id` | PK |
| `gacha_campaign_id` | 關聯活動 FK |
| `user_id` | 中獎用戶 ID |
| `prize_id` | 中獎獎項 ID |
| `created_at` | 紀錄建立時間 |

### 5.2 Redis 快取 Key-Value 設計

* **機率配置 (Hash)**: `gacha:campaign:{id}:prizes`
* **剩餘庫存 (Integer)**: `gacha:prize:{id}:stock`
* **冪等性防護 (String)**: `gacha:idempotency:{uuid}`

### 5.3 Kafka Event Payload

* **Topic**: `gacha.reward.granted`
* **Message**:

  {
  "event\_id": "550e8400-e29b-41d4-a716-446655440000",
  "user\_id": "u123",
  "gacha\_campaign\_id": "c1",
  "prize\_id": "p1",
  "timestamp": 1718000000
  }

## 6. API 介面規格 (API Specifications)

### \[Admin 端]

**1. POST `/v1/admin/gachas` (建立活動)**

* **Payload**：需包含活動名稱與獎項陣列。

**2. PUT `/v1/admin/gachas/{id}/prizes` (動態更新)**

* **快取一致性**：更新 MySQL 後，嚴格執行 `DEL gacha:campaign:{id}:prizes`，下次抽獎將觸發 Singleflight 重新載入。

### \[Client 端]

**1. POST `/v1/gachas/{id}/draw` (執行抽獎)**

* **Headers**: `X-User-Id: {user_id}`, `Idempotency-Key: {uuid}`
  > **💡 身分驗證架構設計**：本服務預期部署在 API Gateway 後方。API Gateway 負責集中式的 Token 驗證（如 JWT 或 Session），驗證成功後將用戶識別（User ID）以明碼透過 `X-User-Id` Header 往下傳遞給本抽獎微服務。這使本服務得以保持無狀態 (Stateless)、專注於高併發抽獎業務與效能。

## 7. 專案目錄結構 (Folder Structure)

```
momo-gacha/
├── cmd/
│   ├── api/main.go               # Gacha API Service 進入點
│   └── worker/main.go            # Reach Worker Service 進入點
├── internal/
│   ├── handler/                  # Delivery 層：HTTP Request 解析
│   ├── usecase/                  # [核心心臟] 獨立業務情境 (包含雙層引擎邏輯與 Singleflight)
│   ├── repository/               # Data 層：MySQL/Redis 讀寫與 Lua Script
│   ├── mq/                       # Event 層：Kafka Producer/Consumer 實作
│   ├── worker/                   # Worker 層：處理非同步批次落庫與觸達發送模擬
│   └── domain/                   # Domain 層：跨層共用 Structs
└── ...
```

## 8. SLA 驗證與測試策略 (SLA Validation & Testing)

本專案透過三層防線互補驗證上述設計的可行性：

### 8.1 微基準測試 (Micro-Benchmark)
排除網路與磁碟 I/O，驗證 Go 應用層核心邏輯效能（可於本機執行 `make bench`）：
* 隨機演算法 (`selectPrizeLayer1`): **13 ns/op** (單核 $\approx 7,000\text{萬 QPS}$)
* 抽獎 Usecase 核心流控 (`Draw`): **423 ns/op** (單核 $\approx 230\text{萬 QPS}$)
* *註：此測試隔離了外部 I/O。網路延遲 (RTT) 與磁碟 I/O 實務上已透過 Lua 腳本減量（多個操作併為 1 次 RTT）以及 Kafka 異步削峰與批次落庫平滑處理。*

### 8.2 端到端整合測試 (E2E Test)
本地 Docker-compose 因共享單機 CPU/IO 資源，腳本設定 `concurrencyLimit=10` 以防磁碟競爭超時。旨在模擬高併發以驗證「冪等防連點、零超賣降級、落庫最終一致性」等業務邏輯正確性。

### 8.3 生產環境壓力測試規劃 (Production Load Test)
* **分散式壓測**：規劃使用常見壓測工具（如 k6 / Locust）部署於叢集多個節點，避免壓測端單機 CPU 成為流量瓶頸。
* **指標監控**：使用 Prometheus 搭配 Grafana，重點監控 **Redis CPU**（監控 Lua 腳本是否吃滿單執行緒核心）與 **Kafka Consumer Lag**（積壓指標，Lag 迅速收斂回零代表最終一致性同步平滑）。
* **混沌測試**：在持續加壓中手動模擬快取中斷或節點故障，驗證 API 能否自動熔斷降級（全部走保底，無報錯且零超賣）並在快取重啟後自癒。

## 9. 未來展望與工程規劃 (Future Roadmap)

* **Transactional Outbox 與新串 Kafka 觸達流水線**：為了解決 Redis 扣減與 Kafka 發送之間可能發生的不一致，並確保第三方通知服務在瞬間高併發流量下受到流量保護。未來規劃採用 Outbox 模式：API 開獎事件先推送至 `Topic-1`，落庫 Worker 消費並在 DB 事務中同步 Commit 業務明細與 `outbox` 記錄。隨後由獨立 Relay 撈取變更發布至觸達專用 `Topic-2`，最後由通知 Consumer 訂閱 `Topic-2` 進行流量控制發送，確保 MySQL 成功落庫後才派發，並極致保護第三方 API 免於流量過載。
* **持久層框架升級 (GORM)**：採用混合模式。Admin CRUD 引入 GORM 提升開發效率；高併發核心抽獎與 Worker 批次落庫維持原生 SQL 以求極致性能。
* **可觀測性建置 (Observability)**：導入 OpenTelemetry 進行鏈路追蹤，並使用 Prometheus / Grafana 監控 Redis 庫存與 Kafka Consumer Lag。

