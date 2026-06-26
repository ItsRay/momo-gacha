package mq

import (
	"context"
	"encoding/json"
	"momo-gacha/internal/domain"

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

	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(event.UserID),
		Value: data,
	})
}

func (p *kafkaPublisher) Close() error {
	return p.writer.Close()
}
