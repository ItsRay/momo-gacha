package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
	fmt.Println("🚀 momo-gacha End-to-End Stress & Verification Tool")
	fmt.Println("==================================================")

	client := &http.Client{Timeout: 5 * time.Second}

	// 1. Create a Gacha Campaign via Admin API
	campaignID := fmt.Sprintf("camp_%d", time.Now().Unix())
	newCampaign := Campaign{
		ID:     campaignID,
		Name:   "618 Momo Brand Gacha",
		Status: "active",
		Prizes: []Prize{
			{ID: "prize_iphone", Type: "limited", Name: "iPhone 17 Pro", ProbBps: 100, InitStock: 5},       // 1% chance, 5 items
			{ID: "prize_coupon", Type: "limited", Name: "100 momo coin", ProbBps: 1000, InitStock: 20},     // 10% chance, 20 items
			{ID: "prize_fallback", Type: "fallback", Name: "銘謝惠顧 / Fallback", ProbBps: 8900, InitStock: 0}, // 89% chance
		},
	}

	campaignJSON, _ := json.Marshal(newCampaign)
	resp, err := client.Post("http://localhost:8080/v1/admin/gachas", "application/json", bytes.NewBuffer(campaignJSON))
	if err != nil {
		fmt.Printf("❌ Failed to contact API Server: %v\n", err)
		fmt.Println("👉 Please ensure 'docker-compose up -d' and both API and Worker services are running!")
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("❌ Failed to create campaign. Status: %d, Body: %s\n", resp.StatusCode, string(body))
		return
	}
	fmt.Printf("✅ Campaign '%s' created successfully via Admin API.\n", campaignID)
	fmt.Println("--------------------------------------------------")

	// 2. Perform concurrent drawing requests (100 users, 200 drawings to exhaust limited stock)
	fmt.Println("🎰 Simulating 150 concurrent draw requests from users...")
	totalDraws := 150
	var wg sync.WaitGroup
	var iphoneWon int64
	var couponWon int64
	var fallbackWon int64
	var duplicatesBlocked int64
	var errs int64

	// Concurrency workers
	concurrencyLimit := 10
	sem := make(chan struct{}, concurrencyLimit)

	for i := 0; i < totalDraws; i++ {
		wg.Add(1)
		go func(userID int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			// Draw request
			req, _ := http.NewRequest("POST", fmt.Sprintf("http://localhost:8080/v1/gachas/%s/draw", campaignID), nil)
			req.Header.Set("X-User-Id", fmt.Sprintf("user_%d", userID))
			// Normal unique idempotency key
			req.Header.Set("Idempotency-Key", fmt.Sprintf("idem_%s_%d", campaignID, userID))

			resp, err := client.Do(req)
			if err != nil {
				atomic.AddInt64(&errs, 1)
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
					case "prize_iphone":
						atomic.AddInt64(&iphoneWon, 1)
					case "prize_coupon":
						atomic.AddInt64(&couponWon, 1)
					case "prize_fallback":
						atomic.AddInt64(&fallbackWon, 1)
					}
				}
			} else {
				atomic.AddInt64(&errs, 1)
			}
		}(i)
	}

	// 3. Simultaneously fire some duplicate idempotency requests to test block capability
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(userID int) {
			defer wg.Done()
			// Send duplicate request for user_0 with the same idempotency key
			req, _ := http.NewRequest("POST", fmt.Sprintf("http://localhost:8080/v1/gachas/%s/draw", campaignID), nil)
			req.Header.Set("X-User-Id", "user_0")
			req.Header.Set("Idempotency-Key", fmt.Sprintf("idem_%s_0", campaignID)) // Duplicate of user 0

			resp, err := client.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()

			// Expect 409 Conflict (or same prize details back depending on when it hits)
			if resp.StatusCode == http.StatusConflict {
				atomic.AddInt64(&duplicatesBlocked, 1)
			}
		}(i)
	}

	wg.Wait()

	fmt.Println("✅ Concurrent draw simulations complete.")
	fmt.Printf("📊 Draw Results:\n")
	fmt.Printf("   - iPhone 17 Pro won: %d (Expected exactly <= 5)\n", atomic.LoadInt64(&iphoneWon))
	fmt.Printf("   - 100 momo coin won: %d (Expected <= 20)\n", atomic.LoadInt64(&couponWon))
	fmt.Printf("   - 銘謝惠顧 (Fallback) won: %d\n", atomic.LoadInt64(&fallbackWon))
	fmt.Printf("   - Idempotency duplicates blocked: %d\n", atomic.LoadInt64(&duplicatesBlocked))
	fmt.Printf("   - Network/HTTP Errors: %d\n", atomic.LoadInt64(&errs))
	fmt.Println("--------------------------------------------------")

	// 4. Query live stock statistics via Admin API
	fmt.Println("⏳ Waiting 1.5 seconds for background worker to persist records and aggregate stocks...")
	time.Sleep(1.5 * time.Second)

	fmt.Println("📈 Querying live stats from Admin API...")
	resp, err = client.Get(fmt.Sprintf("http://localhost:8080/v1/admin/gachas/%s/stats", campaignID))
	if err != nil {
		fmt.Printf("❌ Failed to query stats: %v\n", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		var statResp struct {
			Code    int      `json:"code"`
			Message string   `json:"message"`
			Data    Campaign `json:"data"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&statResp); err == nil {
			fmt.Printf("📝 Live Campaign Stats (Real-Time Redis Stocks Hydrated):\n")
			for _, p := range statResp.Data.Prizes {
				fmt.Printf("   - Prize: %-20s | Initial Stock: %-4d | Remaining Stock: %d\n",
					p.Name, p.InitStock, p.RemainedStock)
			}
		}
	} else {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("❌ Failed to read stats. Status: %d, Body: %s\n", resp.StatusCode, string(body))
	}
	fmt.Println("==================================================")
}
