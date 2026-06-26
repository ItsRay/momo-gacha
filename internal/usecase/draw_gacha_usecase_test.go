package usecase

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"

	"momo-gacha/internal/domain"
)

type mockCampaignRepo struct {
	campaign   *domain.Campaign
	stocks     map[string]*int64 // thread-safe mock stock
	deductCall int64
}

func (m *mockCampaignRepo) CreateCampaign(ctx context.Context, campaign *domain.Campaign) error {
	return nil
}

func (m *mockCampaignRepo) GetCampaign(ctx context.Context, id string) (*domain.Campaign, error) {
	return m.campaign, nil
}

func (m *mockCampaignRepo) GetCampaignWithLiveStock(ctx context.Context, id string) (*domain.Campaign, error) {
	return m.campaign, nil
}

func (m *mockCampaignRepo) UpdatePrizeWeights(ctx context.Context, campaignID string, prizes []domain.Prize) error {
	return nil
}

func (m *mockCampaignRepo) DeductStock(ctx context.Context, campaignID, prizeID string, delta int) (int64, error) {
	atomic.AddInt64(&m.deductCall, 1)
	stockPtr, ok := m.stocks[prizeID]
	if !ok {
		return domain.DeductStockNotFound, nil // Key not found
	}

	for {
		current := atomic.LoadInt64(stockPtr)
		if current < int64(delta) {
			return domain.DeductStockOutOfStock, nil // Out of stock
		}
		if atomic.CompareAndSwapInt64(stockPtr, current, current-int64(delta)) {
			return domain.DeductStockSuccess, nil // Success
		}
	}
}

func (m *mockCampaignRepo) GetPrizeStock(ctx context.Context, prizeID string) (int, error) {
	stockPtr, ok := m.stocks[prizeID]
	if !ok {
		return 0, nil
	}
	return int(atomic.LoadInt64(stockPtr)), nil
}

func (m *mockCampaignRepo) RollbackStock(ctx context.Context, campaignID, prizeID string, delta int) error {
	stockPtr, ok := m.stocks[prizeID]
	if !ok {
		return nil
	}
	atomic.AddInt64(stockPtr, int64(delta))
	return nil
}

type mockPublisher struct {
	events      []domain.RewardEvent
	mu          sync.Mutex
	failPublish bool
}

func (m *mockPublisher) PublishReward(ctx context.Context, event *domain.RewardEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failPublish {
		return fmt.Errorf("simulated MQ failure")
	}
	m.events = append(m.events, *event)
	return nil
}

func (m *mockPublisher) Close() error {
	return nil
}

func TestWeightedRandomDistribution(t *testing.T) {
	// Set up a mock campaign with:
	// - Prize A: 10% (1000 bps)
	// - Prize B: 20% (2000 bps)
	// - Fallback Prize: 70% (auto-allocated 7000 bps)
	campaign := &domain.Campaign{
		ID:     "test_campaign",
		Name:   "Test Gacha",
		Status: domain.CampaignActive,
		Prizes: []domain.Prize{
			{ID: "prize_a", Name: "Prize A", Type: domain.PrizeLimited, ProbBps: 1000},
			{ID: "prize_b", Name: "Prize B", Type: domain.PrizeLimited, ProbBps: 2000},
			{ID: "prize_fallback", Name: "Thank You", Type: domain.PrizeFallback, ProbBps: 0}, // Auto-allocated
		},
	}

	stockA := int64(100000)
	stockB := int64(100000)
	repo := &mockCampaignRepo{
		campaign: campaign,
		stocks: map[string]*int64{
			"prize_a": &stockA,
			"prize_b": &stockB,
		},
	}

	publisher := &mockPublisher{}

	// Mock Redis Client (using mock client or simple in-memory stub)
	// In-memory Redis client via go-redis miniredis or simple connection stub.
	// Since we are mocking redis Client, we can use a local miniredis or a redis client connecting to a dummy client.
	// Wait, we can use an actual Redis client if we connect to a local Redis during integration test,
	// or we can mock redis.Cmdable. Since drawGachaUsecase takes a *redis.Client, we can pass a dummy one if we don't hit Redis,
	// or we can pass a client that will fail/succeed on Get/Set.
	// In our test, if we set idempotencyKey to "", we do NOT touch Redis at all!
	// Let's check draw_gacha_usecase.go:
	// If idempotencyKey == "", it skips Get/SetNX/Set on Redis!
	// So we can pass nil or a closed client and it won't crash! That is extremely clean.
	uc := NewDrawGachaUsecase(repo, publisher, nil)

	iterations := 10000
	results := make(map[string]int)

	for i := 0; i < iterations; i++ {
		prize, err := uc.Draw(context.Background(), "test_campaign", "user_1", "")
		if err != nil {
			t.Fatalf("unexpected draw error at iteration %d: %v", i, err)
		}
		results[prize.ID]++
	}

	t.Logf("Draw results over %d iterations: %v", iterations, results)

	// Check if results roughly match:
	// prize_a ~ 1000 (10%)
	// prize_b ~ 2000 (20%)
	// prize_fallback ~ 7000 (70%)
	checkDistribution(t, results["prize_a"], 0.10, iterations)
	checkDistribution(t, results["prize_b"], 0.20, iterations)
	checkDistribution(t, results["prize_fallback"], 0.70, iterations)
}

func checkDistribution(t *testing.T, count int, expectedProb float64, total int) {
	expectedCount := expectedProb * float64(total)
	stdDev := math.Sqrt(float64(total) * expectedProb * (1 - expectedProb))
	// Allow 4 standard deviations (very safe margin, margin of error < 0.01% chance to fail randomly)
	diff := math.Abs(float64(count) - expectedCount)
	limit := 4 * stdDev
	if diff > limit {
		t.Errorf("distribution out of bounds: expected %.0f, got %d (diff %.0f, max allowed %.0f)",
			expectedCount, count, diff, limit)
	}
}

func TestHighConcurrencyAntiOverselling(t *testing.T) {
	// Campaign has 1 limited prize with stock = 2
	// And a fallback prize
	campaign := &domain.Campaign{
		ID:     "concurrent_campaign",
		Name:   "Concurrent Gacha",
		Status: domain.CampaignActive,
		Prizes: []domain.Prize{
			{ID: "grand_prize", Name: "iPhone 17", Type: domain.PrizeLimited, ProbBps: 10000}, // 100% chance to hitgrand_prize initially
			{ID: "fallback", Name: "銘謝惠顧", Type: domain.PrizeFallback, ProbBps: 0},
		},
	}

	grandStock := int64(2) // Only 2 items available!
	repo := &mockCampaignRepo{
		campaign: campaign,
		stocks: map[string]*int64{
			"grand_prize": &grandStock,
		},
	}

	publisher := &mockPublisher{}
	uc := NewDrawGachaUsecase(repo, publisher, nil)

	// Simulate 100 concurrent drawing users
	numUsers := 100
	var wg sync.WaitGroup
	wg.Add(numUsers)

	resultsChan := make(chan string, numUsers)

	for i := 0; i < numUsers; i++ {
		go func(id int) {
			defer wg.Done()
			userID := fmt.Sprintf("user_%d", id)
			// No idempotency key to bypass redis requirement in unit test
			prize, err := uc.Draw(context.Background(), "concurrent_campaign", userID, "")
			if err != nil {
				t.Errorf("concurrency draw failed for user %s: %v", userID, err)
				return
			}
			resultsChan <- prize.ID
		}(i)
	}

	wg.Wait()
	close(resultsChan)

	grandPrizeWinners := 0
	fallbackWinners := 0

	for prizeID := range resultsChan {
		if prizeID == "grand_prize" {
			grandPrizeWinners++
		} else if prizeID == "fallback" {
			fallbackWinners++
		}
	}

	t.Logf("Concurrency results: grand_prize winners = %d, fallback winners = %d", grandPrizeWinners, fallbackWinners)

	// Check constraints
	if grandPrizeWinners != 2 {
		t.Errorf("expected exactly 2 grand_prize winners, got %d", grandPrizeWinners)
	}
	if fallbackWinners != 98 {
		t.Errorf("expected exactly 98 fallback winners, got %d", fallbackWinners)
	}
	if grandStock != 0 {
		t.Errorf("expected stock to be exactly 0, got %d", grandStock)
	}
}

func TestDrawGachaCompensatingTransaction(t *testing.T) {
	// Campaign has 1 limited prize with stock = 10
	campaign := &domain.Campaign{
		ID:     "compensate_campaign",
		Name:   "Compensate Gacha",
		Status: domain.CampaignActive,
		Prizes: []domain.Prize{
			{ID: "limited_prize", Name: "iPhone 17", Type: domain.PrizeLimited, ProbBps: 10000}, // 100% hit
			{ID: "fallback", Name: "銘謝惠顧", Type: domain.PrizeFallback, ProbBps: 0},
		},
	}

	stockValue := int64(10)
	repo := &mockCampaignRepo{
		campaign: campaign,
		stocks: map[string]*int64{
			"limited_prize": &stockValue,
		},
	}

	// Mock publisher set to FAIL
	publisher := &mockPublisher{failPublish: true}

	uc := NewDrawGachaUsecase(repo, publisher, nil)

	// Draw Gacha. Expect error since MQ publish fails
	_, err := uc.Draw(context.Background(), "compensate_campaign", "user_1", "")
	if err == nil {
		t.Fatalf("expected error due to MQ publish failure, but got nil")
	}

	// Verify that the stock has been rolled back and remains 10
	finalStock, _ := repo.GetPrizeStock(context.Background(), "limited_prize")
	if finalStock != 10 {
		t.Errorf("expected stock to be rolled back to 10, but got %d", finalStock)
	}

	// Verify MQ did not store the event
	if len(publisher.events) != 0 {
		t.Errorf("expected no reward event to be published, but got %d events", len(publisher.events))
	}
}
