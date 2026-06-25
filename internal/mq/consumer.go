package mq

import (
	"context"
	"momo-gacha/internal/domain"
)

type Consumer struct {
	// TODO: Add Redis client or Go channel connection
}

func NewConsumer() *Consumer {
	return &Consumer{}
}

func (c *Consumer) SubscribeReward(handler func(event *domain.RewardEvent) error) error {
	// TODO: Start loop to listen to stream/queue and execute handler
	return nil
}
