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

## 2. 系統承載量評估 (Capacity Planning)

為因應大型電商檔期（如 618）的搶抽熱潮，本系統設計不僅滿足 PRD 要求，更提供超額效能冗餘：

| 指標 | PRD 要求 | 本架構實作目標 | 滿足說明 |
| :--- | :--- | :--- | :--- |
| **峰值 QPS** | 10,000 QPS | 10,000+ QPS | 透過 K8s HPA 水平擴展 API Pods 消化流量 |
| **回應時間** | 1s - 2s | P99 < 50ms | 僅執行 Redis 原子扣減，實現極速回應 |
| **可用性** | 99.99% | 99.99% | 透過斷路器 (Circuit Breaker) 與 Master-Replica 確保高可用 |

**組件乘載力精算：**

* **Gacha API**: 透過 HPA 佈署 5 個 Pods，單一 Pod 負載僅約 2,000 QPS，遠低於單機瓶頸。
* **Redis**: 採用 Lua Script 在記憶體內完成所有計算，維持微秒級延遲。
* **MySQL**: 透過 Worker 批次處理，將寫入負載從 10,000 TPS 壓降至 **約 20 TPS**，徹底根除 DB 崩潰風險。

## 3. 核心技術方案：雙層抽獎引擎 (Core Engine)

為實作 PRD 要求的「兜底降級機制」且維持極致效能，抽獎邏輯拆分為兩層，統一於 `Gacha API Service` 執行：

* **第一層（記憶體權重運算）**：從 Redis 取得獎池基礎配置，自動補滿 `type: fallback` 的兜底機率，利用 Weighted Random 演算法判定初步結果。
* **第二層（Redis 庫存扣減與優雅降級）**：若判定抽中「限量獎項」，攜帶獎品 ID 呼叫 Redis 執行 **Lua Script** 扣減庫存。
  * **扣減成功**：Lua 驗證庫存大於 0 並扣減，回傳 `1`，確立中大獎。
  * **庫存耗盡 (Fallback)**：Lua 發現庫存為 0，回傳 `-2`（工程錯誤碼）。API 層攔截此狀態，不拋出錯誤，而是 **默默將結果置換為「兜底獎項 ID」**，達成平滑防超賣。

## 4. 統一高可用架構與容錯策略 (HA & Fault Tolerance)

為極大化 API 吞吐量並確保架構職責單一，系統全面採用事件驅動架構，以最終一致性 (Eventual Consistency) 作為設計基準。

### 4.1 資料狀態分離與快取重建 (Cache Hydration)

* **活動基礎設定 (機率與規則)**：以 MySQL 為 Source of Truth。若 Redis Cache Miss，實作 `golang.org/x/sync/singleflight`，確保萬人併發下僅單一 Goroutine 查詢資料庫，防禦快取擊穿。
* **高頻異動狀態 (剩餘庫存)**：以 Redis 為 Source of Truth。基礎設施採用 **Master-Replica (主從架構)**。Master 節點關閉 AOF 全力處理高併發；Replica 節點開啟 AOF (`everysec`) 進行非同步磁碟備份。

### 4.2 Kafka 消費與 MySQL 落庫管線 (Consume & Write Pipeline)

為兼顧高吞吐量與資料強一致性，`Reach Worker` 實作以下管線與防誤殺機制：

1. **雙條件批次拉取 (Batching)**：Worker 設定閥值為 `BatchSize = 500` 或 `Timeout = 200ms`。達標後於記憶體中依 `prize_id` 進行彙總（例如將 80 個相同的扣減合併為單句 SQL 樂觀鎖更新）。
2. **批次交易執行 (Batch Transaction)**：開啟 DB Transaction，執行 `Bulk Insert logs` 與彙總後的樂觀鎖扣減。若成功則 Commit 並向 Kafka 提交 Offset (ACK)。
3. **異常降級與精準隔離 (Batch-to-Single Fallback)**：當「彙總扣減量大於剩餘庫存（大獎售罄邊界）」時，批次交易 Rollback。Worker 將該批次降級為 **單筆循序執行 (Sequential Process)**。正常訊息順利 Commit；導致超賣的異常訊息則被精準隔離並轉發至 **DLQ (Dead Letter Queue)**，確保合法訊息絕不被連坐誤殺。

### 4.3 業務補償機制與斷路器防護 (Business Fallback & Fail-Fast)

* **觸發業務降級 (Business Fallback)**：針對進入 DLQ 的超賣訂單（如 Redis 硬體災難遺失資料導致的溢出），系統將自動觸發營運降級流程（派發等值點數並發送致歉通知），以商業手段換取主流程效能。
* **防重複抽獎**：API 要求 Header 帶入 `Idempotency-Key`。透過 Redis `SETNX` 攔截網路重試或惡意連點，防止重複扣除代幣。
* **快速失敗**：若 Redis 叢集癱瘓，API 優先觸發 Circuit Breaker 直接回傳 `HTTP 503`。提早中斷請求，不再執行後續冪等檢查與抽獎，絕對保護用戶資產。

## 5. 資料模型與基礎設施設計 (Data Model)

### 5.1 MySQL 關聯式模型 (Source of Truth)

採用 MySQL 作為底層儲存，依賴其 ACID 特性與行級鎖 (Row-Level Lock)，作為財務對帳與庫存樂觀鎖的最終防線：

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

**gacha\_reward\_logs (中獎明細軌跡表)**
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
│   └── domain/                   # Domain 層：跨層共用 Structs
└── ...
```
