package mq

import (
	"context"
	"encoding/json"
	"momo-gacha/internal/domain"
	"momo-gacha/pkg/logger"
	"time"

	"github.com/segmentio/kafka-go"
)

type kafkaPublisher struct {
	writer *kafka.Writer
}

// NewKafkaPublisher creates a new domain.MessagePublisher implementation.
func NewKafkaPublisher(brokers []string, topic string) domain.MessagePublisher {
	return &kafkaPublisher{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		},
	}
}

func (p *kafkaPublisher) PublishReward(ctx context.Context, event *domain.RewardEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		lastErr = p.writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(event.UserID),
			Value: data,
		})
		if lastErr == nil {
			return nil
		}
		logger.Warn("Failed to publish reward event to MQ (attempt %d/3): %v", attempt, lastErr)
		if attempt < 3 {
			time.Sleep(50 * time.Millisecond)
		}
	}

	logger.Error("Failed to publish reward event to MQ after 3 attempts: %v", lastErr)
	return lastErr
}

func (p *kafkaPublisher) Close() error {
	return p.writer.Close()
}
