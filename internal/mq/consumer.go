package mq

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"momo-gacha/internal/domain"
	"time"

	"github.com/segmentio/kafka-go"
)

type kafkaConsumer struct {
	reader *kafka.Reader
}

// NewKafkaConsumer creates a new domain.MessageConsumer implementation.
func NewKafkaConsumer(brokers []string, topic, groupID string) domain.MessageConsumer {
	return &kafkaConsumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  brokers,
			GroupID:  groupID,
			Topic:    topic,
			MinBytes: 10e3, // 10KB
			MaxBytes: 10e6, // 10MB
		}),
	}
}

func (c *kafkaConsumer) ConsumeEvents(ctx context.Context, batchSize int, timeout time.Duration, handler func(ctx context.Context, events []domain.RewardEvent) error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			batchMessages := make([]kafka.Message, 0, batchSize)
			batchEvents := make([]domain.RewardEvent, 0, batchSize)

			for len(batchEvents) < batchSize {
				var msg kafka.Message
				var err error

				if len(batchEvents) == 0 {
					// Block waiting for the first message of the batch
					msg, err = c.reader.FetchMessage(ctx)
					if err != nil {
						if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || err == io.EOF {
							return nil
						}
						return err
					}
				} else {
					// Subsequent messages are fetched with a timeout
					fetchCtx, cancel := context.WithTimeout(ctx, timeout)
					msg, err = c.reader.FetchMessage(fetchCtx)
					cancel()
					if err != nil {
						// Timeout reached, process the current batch
						break
					}
				}

				var ev domain.RewardEvent
				if err := json.Unmarshal(msg.Value, &ev); err != nil {
					// Skip unparseable messages but log or handle them
					continue
				}

				batchMessages = append(batchMessages, msg)
				batchEvents = append(batchEvents, ev)
			}

			if len(batchEvents) == 0 {
				continue
			}

			// Invoke the batch handler.
			// The handler must process all events (or fallback to DLQ for failures) and return nil
			// so that we can safely commit the batch.
			err := handler(ctx, batchEvents)
			if err != nil {
				// If the handler still returns an error, we do not commit and return the error.
				return err
			}

			// Commit all messages in the batch
			if err := c.reader.CommitMessages(ctx, batchMessages...); err != nil {
				return err
			}
		}
	}
}

func (c *kafkaConsumer) Close() error {
	return c.reader.Close()
}
