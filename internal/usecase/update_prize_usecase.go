package usecase

import (
	"context"
	"momo-gacha/internal/domain"
)

type UpdatePrizeUsecase interface {
	UpdatePrizeWeights(ctx context.Context, campaignID string, prizes []domain.Prize) error
}

type updatePrizeUsecase struct {
	repo domain.CampaignRepository
}

func NewUpdatePrizeUsecase(repo domain.CampaignRepository) UpdatePrizeUsecase {
	return &updatePrizeUsecase{
		repo: repo,
	}
}

func (u *updatePrizeUsecase) UpdatePrizeWeights(ctx context.Context, campaignID string, prizes []domain.Prize) error {
	// TODO: 1. Validate total weight sum if needed, verify fallback prize exists
	// TODO: 2. Update persistent storage/redis cache
	// TODO: 3. Invalidate Redis cache to ensure weight updates are applied instantly
	return nil
}
