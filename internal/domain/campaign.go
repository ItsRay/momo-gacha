package domain

import (
	"context"
	"time"
)

// CampaignStatus represents the status of a campaign.
type CampaignStatus string

const (
	CampaignDraft  CampaignStatus = "draft"
	CampaignActive CampaignStatus = "active"
	CampaignEnded  CampaignStatus = "ended"
)

// PrizeType represents the type of a prize.
type PrizeType string

const (
	PrizeLimited  PrizeType = "limited"
	PrizeFallback PrizeType = "fallback"
)

// Campaign represents the gacha campaign model.
type Campaign struct {
	ID        string         `json:"id" db:"id"`
	Name      string         `json:"name" db:"name"`
	Status    CampaignStatus `json:"status" db:"status"`
	Prizes    []Prize        `json:"prizes,omitempty"`
	CreatedAt time.Time      `json:"created_at" db:"created_at"`
	UpdatedAt time.Time      `json:"updated_at" db:"updated_at"`
}

// Prize represents the gacha prize configuration and state.
type Prize struct {
	ID            string    `json:"id" db:"id"`
	CampaignID    string    `json:"gacha_campaign_id" db:"gacha_campaign_id"`
	Type          PrizeType `json:"type" db:"type"`
	Name          string    `json:"name" db:"name"`
	ProbBps       int       `json:"prob_bps" db:"prob_bps"` // Probability in basis points (10000 = 100%, 100 = 1%)
	InitStock     int       `json:"init_stock" db:"init_stock"`
	RemainedStock int       `json:"remained_stock" db:"remained_stock"`
	CreatedAt     time.Time `json:"created_at" db:"created_at"`
	UpdatedAt     time.Time `json:"updated_at" db:"updated_at"`
}

// RewardRecord represents the persistent record of a won prize.
type RewardRecord struct {
	ID         string    `json:"id" db:"id"`
	CampaignID string    `json:"gacha_campaign_id" db:"gacha_campaign_id"`
	UserID     string    `json:"user_id" db:"user_id"`
	PrizeID    string    `json:"prize_id" db:"prize_id"`
	CreatedAt  time.Time `json:"created_at" db:"created_at"`
}

// CampaignRepository manages campaigns and prizes.
type CampaignRepository interface {
	// CreateCampaign creates a new campaign and its associated prizes in persistent storage.
	CreateCampaign(ctx context.Context, campaign *Campaign) error

	// GetCampaign retrieves a campaign and its prizes.
	GetCampaign(ctx context.Context, id string) (*Campaign, error)

	// UpdatePrizeWeights updates prize weights in persistent storage.
	UpdatePrizeWeights(ctx context.Context, campaignID string, prizes []Prize) error

	// DeductStock atomically deducts a specified amount of stock for a prize.
	// Returns:
	//   1: Success (stock decremented successfully)
	//  -1: Error (key/prize does not exist)
	//  -2: Out of stock (requested delta exceeds remaining stock)
	DeductStock(ctx context.Context, campaignID, prizeID string, delta int) (int64, error)

	// GetPrizeStock retrieves the current remaining stock of a prize.
	// Returns the current stock count and an error if any.
	GetPrizeStock(ctx context.Context, prizeID string) (int, error)
}

// RewardRepository handles persistence of reward records and stock reconciliation.
type RewardRepository interface {
	// ExecuteBatchTransaction performs batch persistence of reward records and stock deductions in a single transaction.
	ExecuteBatchTransaction(ctx context.Context, records []RewardRecord, stockDeductions map[string]int) error

	// InsertRewardRecord inserts a single reward record. 
	// Used in the sequential fallback path when a batch transaction fails and must be processed item-by-item.
	InsertRewardRecord(ctx context.Context, record *RewardRecord) error

	// DeductPrizeStock deducts the stock of a single prize in persistent storage.
	DeductPrizeStock(ctx context.Context, prizeID string, deductCount int) error
}
