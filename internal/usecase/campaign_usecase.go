package usecase

import (
	"context"
	"errors"
	"momo-gacha/internal/domain"
)

// CampaignUsecase handles administrative business actions for campaigns and prizes.
type CampaignUsecase interface {
	CreateCampaign(ctx context.Context, campaign *domain.Campaign) error
	UpdatePrizeWeights(ctx context.Context, campaignID string, prizes []domain.Prize) error
	// GetCampaignStats 獲取包含最新動態庫存的活動詳情。
	// 專門用於後台監控端點；絕不在高頻抽獎主路徑中呼叫，以防造成額外的讀取負載。
	GetCampaignStats(ctx context.Context, campaignID string) (*domain.Campaign, error)
}

type campaignUsecase struct {
	campaignRepo domain.CampaignRepository
}

// NewCampaignUsecase creates a new CampaignUsecase.
func NewCampaignUsecase(campaignRepo domain.CampaignRepository) CampaignUsecase {
	return &campaignUsecase{
		campaignRepo: campaignRepo,
	}
}

func (u *campaignUsecase) CreateCampaign(ctx context.Context, campaign *domain.Campaign) error {
	// 1. Basic validation
	if campaign.Name == "" {
		return domain.NewValidationError("campaign name cannot be empty")
	}
	if len(campaign.Prizes) == 0 {
		return domain.NewValidationError("campaign must have at least one prize")
	}

	// Check if campaign already exists
	existing, err := u.campaignRepo.GetCampaign(ctx, campaign.ID)
	if err == nil && existing != nil {
		return domain.NewConflictError("campaign already exists")
	}

	// 2. Validate fallback prize exists and weights total
	if err := u.validatePrizes(campaign.Prizes); err != nil {
		return err
	}

	// 3. Persist to storage
	return u.campaignRepo.CreateCampaign(ctx, campaign)
}

func (u *campaignUsecase) UpdatePrizeWeights(ctx context.Context, campaignID string, prizes []domain.Prize) error {
	// 1. Basic validation
	if len(prizes) == 0 {
		return domain.NewValidationError("prizes list cannot be empty")
	}

	// Check if campaign exists
	existing, err := u.campaignRepo.GetCampaign(ctx, campaignID)
	if err != nil {
		return err
	}
	if existing == nil {
		return domain.NewValidationError("campaign not found")
	}

	// 2. Validate fallback prize exists and weights total
	if err := u.validatePrizes(prizes); err != nil {
		return err
	}

	// 3. Update persistent weights
	if err := u.campaignRepo.UpdatePrizeWeights(ctx, campaignID, prizes); err != nil {
		return err
	}

	return nil
}

// GetCampaignStats 批次獲取活動設定與最新的動態即時庫存。
// 此方法專為營運管理後台設計，不適用於高併發抽獎流程。
func (u *campaignUsecase) GetCampaignStats(ctx context.Context, campaignID string) (*domain.Campaign, error) {
	campaign, err := u.campaignRepo.GetCampaignWithLiveStock(ctx, campaignID)
	if err != nil {
		return nil, err
	}
	if campaign == nil {
		return nil, domain.NewValidationError("campaign not found")
	}

	return campaign, nil
}

// validatePrizes checks business constraints on gacha prizes.
func (u *campaignUsecase) validatePrizes(prizes []domain.Prize) error {
	fallbackCount := 0
	var totalWeight int

	for _, prize := range prizes {
		if prize.Type == domain.PrizeFallback {
			fallbackCount++
		}
		totalWeight += prize.ProbBps
	}

	if fallbackCount != 1 {
		return domain.NewValidationError("campaign must configure exactly one fallback prize")
	}
	if totalWeight > domain.MaxBasisPoints {
		return domain.NewValidationError("total weight of prizes cannot exceed 10000 (100%)")
	}

	return nil
}
