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
		return errors.New("campaign name cannot be empty")
	}
	if len(campaign.Prizes) == 0 {
		return errors.New("campaign must have at least one prize")
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
		return errors.New("prizes list cannot be empty")
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

func (u *campaignUsecase) GetCampaignStats(ctx context.Context, campaignID string) (*domain.Campaign, error) {
	// 1. Fetch campaign from DB/Cache
	campaign, err := u.campaignRepo.GetCampaign(ctx, campaignID)
	if err != nil {
		return nil, err
	}
	if campaign == nil {
		return nil, errors.New("campaign not found")
	}

	// 2. Hydrate dynamic live stocks for each prize
	for i, prize := range campaign.Prizes {
		stock, err := u.campaignRepo.GetPrizeStock(ctx, prize.ID)
		if err == nil {
			// If cached, override the DB remained stock with the live stock
			campaign.Prizes[i].RemainedStock = stock
		}
	}

	return campaign, nil
}

// validatePrizes checks business constraints on gacha prizes.
func (u *campaignUsecase) validatePrizes(prizes []domain.Prize) error {
	hasFallback := false
	var totalWeight int

	for _, prize := range prizes {
		if prize.Type == domain.PrizeFallback {
			hasFallback = true
		}
		totalWeight += prize.ProbBps
	}

	if !hasFallback {
		return errors.New("campaign must configure at least one fallback prize")
	}
	if totalWeight > 10000 {
		return errors.New("total weight of prizes cannot exceed 10000 (100%)")
	}

	return nil
}
