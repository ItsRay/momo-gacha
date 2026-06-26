package usecase

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"time"

	"momo-gacha/internal/domain"

	"github.com/redis/go-redis/v9"
)

// DrawGachaUsecase handles the client lottery request with idempotency.
type DrawGachaUsecase interface {
	Draw(ctx context.Context, campaignID string, userID string, idempotencyKey string) (*domain.Prize, error)
}

type drawGachaUsecase struct {
	campaignRepo domain.CampaignRepository
	publisher    domain.MessagePublisher
	rdb          *redis.Client
}

func NewDrawGachaUsecase(campaignRepo domain.CampaignRepository, publisher domain.MessagePublisher, rdb *redis.Client) DrawGachaUsecase {
	return &drawGachaUsecase{
		campaignRepo: campaignRepo,
		publisher:    publisher,
		rdb:          rdb,
	}
}

func (u *drawGachaUsecase) Draw(ctx context.Context, campaignID string, userID string, idempotencyKey string) (*domain.Prize, error) {
	// 1. Idempotency Check
	idempotencyRedisKey := fmt.Sprintf("gacha:idempotency:%s", idempotencyKey)
	if idempotencyKey != "" {
		val, err := u.rdb.Get(ctx, idempotencyRedisKey).Result()
		if err == nil {
			if val == "processing" {
				return nil, errors.New("request is being processed, please try again later")
			}
			// If it's a cached prize result, return it immediately
			var cachedPrize domain.Prize
			if err := json.Unmarshal([]byte(val), &cachedPrize); err == nil {
				return &cachedPrize, nil
			}
		}

		// Try to acquire lock
		acquired, err := u.rdb.SetNX(ctx, idempotencyRedisKey, "processing", 10*time.Second).Result()
		if err != nil {
			return nil, fmt.Errorf("idempotency check failed: %w", err)
		}
		if !acquired {
			return nil, errors.New("duplicate request")
		}
	}

	// Helper to release/clean lock in case of failure
	failCleanLock := func() {
		if idempotencyKey != "" {
			_ = u.rdb.Del(ctx, idempotencyRedisKey).Err()
		}
	}

	// 2. Fetch Campaign
	campaign, err := u.campaignRepo.GetCampaign(ctx, campaignID)
	if err != nil {
		failCleanLock()
		return nil, fmt.Errorf("failed to retrieve campaign: %w", err)
	}
	if campaign == nil {
		failCleanLock()
		return nil, errors.New("campaign not found")
	}
	if campaign.Status != domain.CampaignActive {
		failCleanLock()
		return nil, errors.New("campaign is not active")
	}

	// 3. Dynamic Fallback Weight Allocation & Weighted Random Selection (Layer 1)
	wonPrize, err := u.selectPrizeLayer1(campaign)
	if err != nil {
		failCleanLock()
		return nil, err
	}

	// 4. Redis Stock Deduction & Fallback (Layer 2)
	finalPrize := wonPrize
	if wonPrize.Type == domain.PrizeLimited {
		// Atomic stock check and deduction in Redis
		res, err := u.campaignRepo.DeductStock(ctx, campaign.ID, wonPrize.ID, 1)
		if err != nil {
			failCleanLock()
			return nil, fmt.Errorf("failed to deduct stock: %w", err)
		}

		if res == -2 || res == -1 {
			// -2: Out of stock, -1: Key does not exist. Trigger Graceful Fallback!
			fallbackPrize, err := u.findFallbackPrize(campaign)
			if err != nil {
				failCleanLock()
				return nil, err
			}
			finalPrize = fallbackPrize
		}
	}

	// 5. Publish RewardEvent to Kafka
	eventID := fmt.Sprintf("event_%d_%s", time.Now().UnixNano(), userID)
	event := domain.RewardEvent{
		EventID:    eventID,
		UserID:     userID,
		CampaignID: campaign.ID,
		PrizeID:    finalPrize.ID,
		PrizeName:  finalPrize.Name,
		Timestamp:  time.Now().Unix(),
	}

	err = u.publisher.PublishReward(ctx, &event)
	if err != nil {
		// If publishing fails, we must release the idempotency lock so client can retry.
		failCleanLock()
		return nil, fmt.Errorf("failed to publish reward event: %w", err)
	}

	// 6. Save Idempotent Result in Redis (24 Hours)
	if idempotencyKey != "" {
		prizeData, err := json.Marshal(finalPrize)
		if err == nil {
			_ = u.rdb.Set(ctx, idempotencyRedisKey, string(prizeData), 24*time.Hour).Err()
		}
	}

	return finalPrize, nil
}

func (u *drawGachaUsecase) selectPrizeLayer1(campaign *domain.Campaign) (*domain.Prize, error) {
	// Find fallback prize and sum other prizes' weights
	var fallbackIdx = -1
	var otherWeightsSum int

	for i, p := range campaign.Prizes {
		if p.Type == domain.PrizeFallback {
			fallbackIdx = i
		} else {
			otherWeightsSum += p.ProbBps
		}
	}

	if fallbackIdx == -1 {
		return nil, errors.New("invalid campaign configuration: no fallback prize found")
	}

	// Dynamically calculate fallback weight: 10000 - sum(other weights)
	fallbackWeight := 10000 - otherWeightsSum
	if fallbackWeight < 0 {
		fallbackWeight = 0 // In case other weights exceed 10000
	}

	// Weighted Random Selection
	// Generate random number r in [0, 10000)
	nBig, err := rand.Int(rand.Reader, big.NewInt(10000))
	if err != nil {
		return nil, fmt.Errorf("failed to generate random number: %w", err)
	}
	r := int(nBig.Int64())

	var currentSum int
	for i, p := range campaign.Prizes {
		weight := p.ProbBps
		if i == fallbackIdx {
			weight = fallbackWeight
		}
		currentSum += weight
		if r < currentSum {
			return &campaign.Prizes[i], nil
		}
	}

	// In case of rounding or edge cases, return the fallback prize
	return &campaign.Prizes[fallbackIdx], nil
}

func (u *drawGachaUsecase) findFallbackPrize(campaign *domain.Campaign) (*domain.Prize, error) {
	for i, p := range campaign.Prizes {
		if p.Type == domain.PrizeFallback {
			return &campaign.Prizes[i], nil
		}
	}
	return nil, errors.New("no fallback prize configured for campaign")
}
