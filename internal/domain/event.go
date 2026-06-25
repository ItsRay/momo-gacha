package domain

import "time"

// RewardEvent represents the event published after a successful prize draw.
type RewardEvent struct {
	DrawID     string    `json:"draw_id"`
	UserID     string    `json:"user_id"`
	CampaignID string    `json:"campaign_id"`
	PrizeID    string    `json:"prize_id"`
	PrizeName  string    `json:"prize_name"`
	DrawTime   time.Time `json:"draw_time"`
}

// MessageQueue defines interfaces for event MQ.
type MessageQueue interface {
	PublishReward(event *RewardEvent) error
	SubscribeReward(handler func(event *RewardEvent) error) error
}
