package redis

import (
	"context"
	"momo-gacha/internal/domain"
)

type GachaRepository struct {
	// TODO: Add redis client connection
}

func NewGachaRepository() domain.CampaignRepository {
	return &GachaRepository{}
}

func (r *GachaRepository) CreateCampaign(campaign *domain.Campaign) error {
	// TODO: Implement campaign and prizes storage (e.g., hash, set, etc.)
	return nil
}

func (r *GachaRepository) GetCampaign(id string) (*domain.Campaign, error) {
	// TODO: Implement get campaign and prizes list
	return nil, nil
}

func (r *GachaRepository) UpdatePrizeWeights(campaignID string, prizes []domain.Prize) error {
	// TODO: Implement update and cache invalidation
	return nil
}

func (r *GachaRepository) GetPrizeStock(campaignID, prizeID string) (int, error) {
	// TODO: Implement direct stock lookup
	return 0, nil
}

func (r *GachaRepository) DeductStockLua(campaignID, prizeID string) (int64, error) {
	// TODO: Implement evaluation of deduct_stock.lua
	return 0, nil
}
