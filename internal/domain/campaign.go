package domain

import "time"

// Campaign represents a lottery campaign.
type Campaign struct {
	ID        string    `json:"id" db:"id"`
	Name      string    `json:"name" db:"name"`
	StartTime time.Time `json:"start_time" db:"start_time"`
	EndTime   time.Time `json:"end_time" db:"end_time"`
	Prizes    []Prize   `json:"prizes,omitempty"`
}

// Prize represents a prize item in a campaign.
type Prize struct {
	ID          string `json:"id" db:"id"`
	CampaignID  string `json:"campaign_id" db:"campaign_id"`
	Name        string `json:"name" db:"name"`
	Weight      int    `json:"weight" db:"weight"` // Probability weight
	Stock       int    `json:"stock" db:"stock"`   // Remainder stock
	IsLimited   bool   `json:"is_limited" db:"is_limited"`
	IsFallback  bool   `json:"is_fallback" db:"is_fallback"`
}

// CampaignRepository defines data layer interface for campaign and prize.
type CampaignRepository interface {
	CreateCampaign(campaign *Campaign) error
	GetCampaign(id string) (*Campaign, error)
	UpdatePrizeWeights(campaignID string, prizes []Prize) error
	GetPrizeStock(campaignID, prizeID string) (int, error)
	DeductStockLua(campaignID, prizeID string) (int64, error)
}
