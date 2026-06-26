package domain

import (
	"context"
	"time"
)

// RewardEvent represents the payload published for a granted reward.
type RewardEvent struct {
	EventID    string    `json:"event_id"`
	UserID     string    `json:"user_id"`
	CampaignID string    `json:"gacha_campaign_id"`
	PrizeID    string    `json:"prize_id"`
	PrizeName  string    `json:"prize_name"` // Included for display convenience in console / push
	Timestamp  int64     `json:"timestamp"`
}

// MessagePublisher defines the interface to publish reward events (API Service side).
type MessagePublisher interface {
	PublishReward(ctx context.Context, event *RewardEvent) error
	Close() error
}

// MessageConsumer defines the interface to consume reward events (Worker Service side).
type MessageConsumer interface {
	// ConsumeEvents starts a block loop retrieving events and delivering them to handler in batch or single form.
	ConsumeEvents(ctx context.Context, batchSize int, timeout time.Duration, handler func(ctx context.Context, events []RewardEvent) error) error
	Close() error
}
