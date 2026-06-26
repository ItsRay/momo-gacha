package usecase

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"momo-gacha/internal/domain"
	"momo-gacha/pkg/logger"

	"github.com/redis/go-redis/v9"
)

const statusProcessing = "processing"

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
	// 1. 冪等性檢查與鎖定。注意：
	// - idempotencyKey: 前台傳入的原始識別參數字串
	// - idempotencyRedisKey: 格式化後、實際做為 Redis 操作使用的 Key
	idempotencyRedisKey := buildIdempotencyRedisKey(idempotencyKey)
	if idempotencyKey != "" {
		val, err := u.rdb.Get(ctx, idempotencyRedisKey).Result()
		if err == nil {
			if val == statusProcessing {
				return nil, domain.NewConflictError("request is being processed, please try again later")
			}
			// If it's a cached prize result, return it immediately
			var cachedPrize domain.Prize
			if err := json.Unmarshal([]byte(val), &cachedPrize); err == nil {
				return &cachedPrize, nil
			}
		}

		// Try to acquire lock
		acquired, err := u.rdb.SetNX(ctx, idempotencyRedisKey, statusProcessing, 1*time.Minute).Result()
		if err != nil {
			return nil, fmt.Errorf("idempotency check failed: %w", err)
		}
		if !acquired {
			return nil, domain.NewConflictError("duplicate request")
		}
	}

	// Helper to release/clean lock in case of failure
	failCleanLock := func() {
		if idempotencyKey != "" {
			if err := u.rdb.Del(ctx, idempotencyRedisKey).Err(); err != nil {
				logger.Error("failed to release idempotency lock for key %s: %v", idempotencyRedisKey, err)
			}
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
		return nil, domain.NewValidationError("campaign not found")
	}
	if campaign.Status != domain.CampaignActive {
		failCleanLock()
		return nil, domain.NewValidationError("campaign is not active")
	}

	// 3. Dynamic Fallback Weight Allocation & Weighted Random Selection (Layer 1)
	wonPrize, err := u.selectPrizeLayer1(campaign)
	if err != nil {
		failCleanLock()
		return nil, err
	}

	// 4. Redis Stock Deduction & Fallback (Layer 2)
	finalPrize := wonPrize
	var deductSuccess bool
	if wonPrize.Type == domain.PrizeLimited {
		// Atomic stock check and deduction in Redis
		res, err := u.campaignRepo.DeductStock(ctx, campaign.ID, wonPrize.ID, 1)
		if err != nil {
			failCleanLock()
			return nil, fmt.Errorf("failed to deduct stock: %w", err)
		}

		if res == domain.DeductStockSuccess {
			deductSuccess = true
		} else if res == domain.DeductStockOutOfStock || res == domain.DeductStockNotFound {
			// Trigger Graceful Fallback!
			fallbackPrize, err := u.findFallbackPrize(campaign)
			if err != nil {
				failCleanLock()
				return nil, err
			}
			finalPrize = fallbackPrize
		}
	}

	// 5. Publish RewardEvent to MQ (Retries are handled internally by the publisher)
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
		// If publishing fails, we must release the idempotency lock
		failCleanLock()

		// Compensating action: rollback the stock in cache if we successfully deducted it earlier
		if deductSuccess {
			if rerr := u.campaignRepo.RollbackStock(ctx, campaign.ID, wonPrize.ID, 1); rerr != nil {
				logger.Error("failed to rollback stock for prize %s: %v. Cache and storage might be inconsistent.", wonPrize.ID, rerr)
			} else {
				logger.Warn("Compensated stock successfully for prize %s due to MQ publish failure.", wonPrize.ID)
			}
		}
		return nil, fmt.Errorf("failed to publish reward event: %w", err)
	}

	// 6. Save Idempotent Result in Redis (24 Hours)
	if idempotencyKey != "" {
		prizeData, err := json.Marshal(finalPrize)
		if err == nil {
			if err := u.rdb.Set(ctx, idempotencyRedisKey, string(prizeData), 24*time.Hour).Err(); err != nil {
				logger.Error("failed to cache idempotent prize result for key %s: %v", idempotencyRedisKey, err)
			}
		}
	}

	return finalPrize, nil
}

// selectPrizeLayer1 執行第一層抽獎引擎：使用加權隨機演算法選出初步中獎項目。
// 為了簡化邏輯並提高執行效率，此處以所有限量獎品的「總權重和」作為判定臨界值：
// - 當隨機數小於總權重和：代表落入限量獎品區間，我們在迴圈中累加區間，尋找命中哪一個限量獎品。
// - 當隨機數大於等於總權重和：代表直接落入剩餘的保底機率區間，直接回傳保底獎品。
func (u *drawGachaUsecase) selectPrizeLayer1(campaign *domain.Campaign) (*domain.Prize, error) {
	// 遍歷所有獎品，定位唯一保底獎品的索引，並加總限量獎品的權重和
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
		return nil, domain.NewValidationError("invalid campaign configuration: no fallback prize found")
	}

	// 在機率基數 [0, MaxBasisPoints) 區間內產生安全隨機數 r
	nBig, err := rand.Int(rand.Reader, big.NewInt(domain.MaxBasisPoints))
	if err != nil {
		return nil, fmt.Errorf("failed to generate random number: %w", err)
	}
	r := int(nBig.Int64())

	// 1. 若隨機數小於限量獎品權重總和，代表命中限量獎品區間，進一步遍歷找出命中哪一項
	if r < otherWeightsSum {
		var currentSum int
		for i, p := range campaign.Prizes {
			if p.Type != domain.PrizeFallback {
				currentSum += p.ProbBps
				if r < currentSum {
					return &campaign.Prizes[i], nil
				}
			}
		}
	}

	// 2. 否則（隨機數落在限量獎品區間外），代表命中保底區間，直接返回保底獎品
	return &campaign.Prizes[fallbackIdx], nil
}

func (u *drawGachaUsecase) findFallbackPrize(campaign *domain.Campaign) (*domain.Prize, error) {
	for i, p := range campaign.Prizes {
		if p.Type == domain.PrizeFallback {
			return &campaign.Prizes[i], nil
		}
	}
	return nil, domain.NewValidationError("no fallback prize configured for campaign")
}

func buildIdempotencyRedisKey(key string) string {
	return fmt.Sprintf("gacha:idempotency:%s", key)
}
