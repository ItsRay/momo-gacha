package usecase

import (
	"context"
	"fmt"
	"testing"

	"momo-gacha/internal/domain"
)

// BenchmarkSelectPrizeLayer1 測試第一層機率隨機選擇演算法的極限效能
func BenchmarkSelectPrizeLayer1(b *testing.B) {
	campaign := &domain.Campaign{
		ID:     "bench_campaign",
		Name:   "Benchmark Campaign",
		Status: domain.CampaignActive,
		Prizes: []domain.Prize{
			{ID: "prize_1", Name: "iPhone 17 Pro", Type: domain.PrizeLimited, ProbBps: 100},   // 1%
			{ID: "prize_2", Name: "100 momo coin", Type: domain.PrizeLimited, ProbBps: 1000}, // 10%
			{ID: "prize_fallback", Name: "銘謝惠顧", Type: domain.PrizeFallback, ProbBps: 0}, // 89% (保底)
		},
	}

	repo := &mockCampaignRepo{campaign: campaign}
	publisher := &mockPublisher{}
	uc := NewDrawGachaUsecase(repo, publisher, nil).(*drawGachaUsecase)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = uc.selectPrizeLayer1(campaign)
	}
}

// BenchmarkDrawGachaUsecase 測試在 mock 外部依賴下，Draw 抽獎核心邏輯的單機處理性能上限
func BenchmarkDrawGachaUsecase(b *testing.B) {
	campaign := &domain.Campaign{
		ID:     "bench_campaign",
		Name:   "Benchmark Campaign",
		Status: domain.CampaignActive,
		Prizes: []domain.Prize{
			{ID: "prize_1", Name: "iPhone 17 Pro", Type: domain.PrizeLimited, ProbBps: 100},   // 1%
			{ID: "prize_2", Name: "100 momo coin", Type: domain.PrizeLimited, ProbBps: 1000}, // 10%
			{ID: "prize_fallback", Name: "銘謝惠顧", Type: domain.PrizeFallback, ProbBps: 0}, // 89% (保底)
		},
	}

	stock1 := int64(1000000)
	stock2 := int64(1000000)
	repo := &mockCampaignRepo{
		campaign: campaign,
		stocks: map[string]*int64{
			"prize_1": &stock1,
			"prize_2": &stock2,
		},
	}
	publisher := &mockPublisher{}
	uc := NewDrawGachaUsecase(repo, publisher, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = uc.Draw(context.Background(), "bench_campaign", fmt.Sprintf("user_%d", i), "")
	}
}
