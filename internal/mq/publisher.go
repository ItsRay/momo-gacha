package mq

import (
	"momo-gacha/internal/domain"
)

type Publisher struct {
	// TODO: Add Redis client or Go channel connection
}

func NewPublisher() *Publisher {
	return &Publisher{}
}

func (p *Publisher) PublishReward(event *domain.RewardEvent) error {
	// TODO: Publish event to Redis Stream or a Go channel queue
	return nil
}
