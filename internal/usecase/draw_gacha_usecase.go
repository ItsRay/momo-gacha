package usecase

import (
	"context"
	"momo-gacha/internal/domain"
)

type DrawGachaUsecase interface {
	Draw(ctx context.Context, campaignID, userID string) (*domain.Prize, error)
}

type drawGachaUsecase struct {
	repo domain.CampaignRepository
	mq   domain.MessageQueue
}

func NewDrawGachaUsecase(repo domain.CampaignRepository, mq domain.MessageQueue) DrawGachaUsecase {
	return &drawGachaUsecase{
		repo: repo,
		mq:   mq,
	}
}

func (u *drawGachaUsecase) Draw(ctx context.Context, campaignID, userID string) (*domain.Prize, error) {
	// TODO: 1. Get campaign and its prizes (cached or db)
	// TODO: 2. Perform Weighted Random Algorithm to select a tentative prize
	// TODO: 3. If selected prize is limited, execute Redis Lua script (DeductStockLua)
	// TODO: 4. If Lua returns out-of-stock (-2), perform fallback to the fallback prize (铭谢惠顧 / fallback)
	// TODO: 5. Publish RewardEvent to MQ asynchronously
	// TODO: 6. Return the won prize
	return nil, nil
}
