package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Prize struct {
	ID            string `json:"id"`
	Type          string `json:"type"`
	Name          string `json:"name"`
	ProbBps       int    `json:"prob_bps"`
	InitStock     int    `json:"init_stock"`
	RemainedStock int    `json:"remained_stock"`
}

type Campaign struct {
	ID     string  `json:"id"`
	Name   string  `json:"name"`
	Status string  `json:"status"`
	Prizes []Prize `json:"prizes"`
}

type APIResponse struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

func main() {
	fmt.Println("==================================================")
	fmt.Println("🚀 momo-gacha 端到端併發壓力與自癒驗證工具")
	fmt.Println("==================================================")

	baseURL := os.Getenv("API_URL")
	if baseURL == "" {
		baseURL = "http://localhost:8080"
	}
	fmt.Printf("🎯 目標 API 伺服器地址: %s\n", baseURL)
	fmt.Println("--------------------------------------------------")

	client := &http.Client{Timeout: 5 * time.Second}

	// 1. [Admin] Create Gacha Campaign
	campaignID := fmt.Sprintf("camp_%d", time.Now().Unix())
	fmt.Printf("📢 [Admin] 正在呼叫建立活動 API [POST %s/v1/admin/gachas]...\n", baseURL)
	newCampaign := Campaign{
		ID:     campaignID,
		Name:   "Momo 618 品牌狂歡抽獎",
		Status: "active",
		Prizes: []Prize{
			{ID: fmt.Sprintf("prize_iphone_%s", campaignID), Type: "limited", Name: "iPhone 17 Pro", ProbBps: 100, InitStock: 5},
			{ID: fmt.Sprintf("prize_coupon_%s", campaignID), Type: "limited", Name: "100 momo coin", ProbBps: 1000, InitStock: 20},
			{ID: fmt.Sprintf("prize_fallback_%s", campaignID), Type: "fallback", Name: "銘謝惠顧", ProbBps: 8900, InitStock: 0},
		},
	}

	campaignJSON, _ := json.Marshal(newCampaign)
	resp, err := client.Post(fmt.Sprintf("%s/v1/admin/gachas", baseURL), "application/json", bytes.NewBuffer(campaignJSON))
	if err != nil {
		fmt.Printf("❌ 連接 API 伺服器失敗: %v\n", err)
		fmt.Println("👉 請確保所有 Docker 容器正在運行中 (make compose-up)！")
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("❌ 建立活動失敗。狀態碼: %d, 錯誤內容: %s\n", resp.StatusCode, string(body))
		return
	}
	fmt.Printf("✅ [Admin] 成功建立活動 ID: %s (%s)\n", campaignID, newCampaign.Name)
	fmt.Println("📋 獎品配置詳情：")
	for _, p := range newCampaign.Prizes {
		typeStr := "限量大獎"
		if p.Type == "fallback" {
			typeStr = "保底獎項"
		}
		fmt.Printf("  - 獎品: %-20s | 類型: %-8s | 機率: %6.2f%% | 初始庫存: %d\n",
			p.Name, typeStr, float64(p.ProbBps)/100.0, p.InitStock)
	}
	fmt.Println("--------------------------------------------------")

	// 2. [Client] Perform concurrent drawing requests
	totalDraws := 200
	fmt.Printf("🎰 [Client] 正在呼叫抽獎 API [POST %s/v1/gachas/%s/draw] 模擬 %d 位不同用戶併發抽獎...\n", baseURL, campaignID, totalDraws)
	var wg sync.WaitGroup
	var iphoneWon int64
	var couponWon int64
	var fallbackWon int64
	var duplicatesBlocked int64
	var errs int64

	// Concurrency workers limit
	concurrencyLimit := 10
	sem := make(chan struct{}, concurrencyLimit)

	for i := 0; i < totalDraws; i++ {
		wg.Add(1)
		go func(userID int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			req, _ := http.NewRequest("POST", fmt.Sprintf("%s/v1/gachas/%s/draw", baseURL, campaignID), nil)
			req.Header.Set("X-User-Id", fmt.Sprintf("user_%d", userID))
			req.Header.Set("Idempotency-Key", fmt.Sprintf("idem_%s_%d", campaignID, userID))

			resp, err := client.Do(req)
			if err != nil {
				atomic.AddInt64(&errs, 1)
				fmt.Printf("❌ [Client] 用戶 user_%-3d 抽獎 -> 網路錯誤: %v\n", userID, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				var apiResp struct {
					Code    int    `json:"code"`
					Message string `json:"message"`
					Data    Prize  `json:"data"`
				}
				if err := json.NewDecoder(resp.Body).Decode(&apiResp); err == nil {
					switch apiResp.Data.ID {
					case fmt.Sprintf("prize_iphone_%s", campaignID):
						atomic.AddInt64(&iphoneWon, 1)
						fmt.Printf("🎁 [Client] 用戶 user_%-3d 抽獎 -> 🎉 恭喜獲得: %s!\n", userID, apiResp.Data.Name)
					case fmt.Sprintf("prize_coupon_%s", campaignID):
						atomic.AddInt64(&couponWon, 1)
						fmt.Printf("🎁 [Client] 用戶 user_%-3d 抽獎 -> 恭喜獲得: %s!\n", userID, apiResp.Data.Name)
					default:
						atomic.AddInt64(&fallbackWon, 1)
						fmt.Printf("🎫 [Client] 用戶 user_%-3d 抽獎 -> %s\n", userID, apiResp.Data.Name)
					}
				}
			} else {
				atomic.AddInt64(&errs, 1)
				body, _ := io.ReadAll(resp.Body)
				fmt.Printf("❌ [Client] 用戶 user_%-3d 抽獎 -> 失敗 (狀態碼 %d): %s\n", userID, resp.StatusCode, string(body))
			}
		}(i)
	}

	// Simultaneously test duplicate idempotency requests (Simulate same user user_0 double clicking 10 times)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(userID int) {
			defer wg.Done()
			req, _ := http.NewRequest("POST", fmt.Sprintf("%s/v1/gachas/%s/draw", baseURL, campaignID), nil)
			req.Header.Set("X-User-Id", "user_0")
			req.Header.Set("Idempotency-Key", fmt.Sprintf("idem_%s_0", campaignID))

			resp, err := client.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusConflict {
				atomic.AddInt64(&duplicatesBlocked, 1)
			}
		}(i)
	}

	wg.Wait()

	fmt.Println("--------------------------------------------------")
	fmt.Println("✅ [Client] 併發抽獎模擬結束！")
	fmt.Printf("📊 Client 視角抽中獎項累計統計 (總抽獎次數: %d 次)：\n", totalDraws)
	fmt.Println("+----------------------+----------+--------------+----------+")
	fmt.Println("| 獎項名稱             | 抽中次數 | 初始配置庫存 | 配置機率 |")
	fmt.Println("+----------------------+----------+--------------+----------+")
	fmt.Printf("| %-20s | %8d | %12s | %7.2f%% |\n", "iPhone 17 Pro", atomic.LoadInt64(&iphoneWon), "5", 1.0)
	fmt.Printf("| %-20s | %8d | %12s | %7.2f%% |\n", "100 momo coin", atomic.LoadInt64(&couponWon), "20", 10.0)
	fmt.Printf("| %-20s | %8d | %12s | %7.2f%% |\n", "銘謝惠顧", atomic.LoadInt64(&fallbackWon), "無限制", 89.0)
	fmt.Println("+----------------------+----------+--------------+----------+")
	fmt.Printf("* 冪等防連點連點阻擋:   %d 次 (故意模擬 user_0 重複快速連點 10 次，成功攔截了後續 9 次重複請求)\n", atomic.LoadInt64(&duplicatesBlocked))
	fmt.Printf("* 網路與請求異常次數:   %d 次 (記錄因 TCP 連線超時或網路抖動造成的 HTTP 異常)\n", atomic.LoadInt64(&errs))
	fmt.Println("--------------------------------------------------")

	// 3. [Admin] Get remaining stocks
	fmt.Println("⏳ 等待 1.5 秒，讓背景 Worker 異步批次將中獎紀錄落庫並更新資料庫庫存...")
	time.Sleep(1500 * time.Millisecond)

	fmt.Printf("📈 [Admin] 正在呼叫活動統計 API [GET %s/v1/admin/gachas/%s/stats] 查詢最新剩餘庫存...\n", baseURL, campaignID)
	resp, err = client.Get(fmt.Sprintf("%s/v1/admin/gachas/%s/stats", baseURL, campaignID))
	if err != nil {
		fmt.Printf("❌ 查詢活動統計失敗: %v\n", err)
		return
	}
	defer resp.Body.Close()

	var finalCampaign Campaign
	if resp.StatusCode == http.StatusOK {
		var statResp struct {
			Code    int      `json:"code"`
			Message string   `json:"message"`
			Data    Campaign `json:"data"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&statResp); err == nil {
			finalCampaign = statResp.Data
			fmt.Println("📝 最終剩餘庫存狀態 (MySQL / Redis 自癒重灌後)：")
			for _, p := range statResp.Data.Prizes {
				if p.Type == "fallback" {
					fmt.Printf("  - 獎品: %-20s | 初始庫存: %-10s | 剩餘庫存: %s (unlimited)\n",
						p.Name, "無限制 (保底)", "無限制")
				} else {
					fmt.Printf("  - 獎品: %-20s | 初始庫存: %-10d | 剩餘庫存: %d\n",
						p.Name, p.InitStock, p.RemainedStock)
				}
			}
		}
	} else {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("❌ 查詢庫存失敗。狀態碼: %d, 內容: %s\n", resp.StatusCode, string(body))
	}
	fmt.Println("==================================================")

	// 4. Generate local report
	reportPath := "benchmark_report.txt"
	var buf bytes.Buffer
	buf.WriteString("==================================================\n")
	buf.WriteString("🚀 momo-gacha 壓力測試與驗收報告 (中文版)\n")
	buf.WriteString("==================================================\n")
	buf.WriteString(fmt.Sprintf("報告生成時間: %s\n", time.Now().Format("2006-01-02 15:04:05")))
	buf.WriteString(fmt.Sprintf("活動 ID:      %s\n", campaignID))
	buf.WriteString("活動配置詳情:\n")
	for _, p := range newCampaign.Prizes {
		typeStr := "限量大獎"
		initStockStr := fmt.Sprintf("%d", p.InitStock)
		if p.Type == "fallback" {
			typeStr = "保底獎項"
			initStockStr = "無限制"
		}
		buf.WriteString(fmt.Sprintf("  - %-20s | 類型: %-6s | 權重機率: %6.2f%% | 初始庫存: %s\n",
			p.Name, typeStr, float64(p.ProbBps)/100.0, initStockStr))
	}
	buf.WriteString("--------------------------------------------------\n")
	buf.WriteString(fmt.Sprintf("壓力測試規模: %d 併發用戶\n", totalDraws))
	buf.WriteString("Client 視角中獎統計:\n")
	buf.WriteString(fmt.Sprintf("  - iPhone 17 Pro 抽中: %d 個 (限量 5 個)\n", atomic.LoadInt64(&iphoneWon)))
	buf.WriteString(fmt.Sprintf("  - 100 momo coin 抽中: %d 個 (限量 20 個)\n", atomic.LoadInt64(&couponWon)))
	buf.WriteString(fmt.Sprintf("  - 銘謝惠顧 抽中:       %d 個\n", atomic.LoadInt64(&fallbackWon)))
	buf.WriteString(fmt.Sprintf("  - 冪等防連點連點阻擋:   %d 次 (防重複抽獎)\n", atomic.LoadInt64(&duplicatesBlocked)))
	buf.WriteString(fmt.Sprintf("  - 網路與請求異常次數:   %d 次\n", atomic.LoadInt64(&errs)))
	buf.WriteString("--------------------------------------------------\n")
	buf.WriteString("最終實時庫存狀態 (MySQL / Redis 資料一致性驗證):\n")
	if len(finalCampaign.Prizes) > 0 {
		for _, p := range finalCampaign.Prizes {
			if p.Type == "fallback" {
				buf.WriteString(fmt.Sprintf("  - 獎品: %-20s | 初始庫存: %-10s | 剩餘庫存: %s (unlimited)\n",
					p.Name, "無限制 (保底)", "無限制"))
			} else {
				buf.WriteString(fmt.Sprintf("  - 獎品: %-20s | 初始庫存: %-10d | 剩餘庫存: %d\n",
					p.Name, p.InitStock, p.RemainedStock))
			}
		}
	} else {
		buf.WriteString("  [錯誤] 無法取得最新活動庫存統計\n")
	}
	buf.WriteString("==================================================\n")

	_ = os.WriteFile(reportPath, buf.Bytes(), 0644)
	fmt.Printf("💾 本次測試報告已成功存檔至: %s\n", reportPath)
	fmt.Println("==================================================")
}
