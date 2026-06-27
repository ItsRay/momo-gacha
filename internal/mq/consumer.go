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
	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	msgChan := make(chan kafka.Message, batchSize)
	errChan := make(chan error, 1)

	// Run background fetch loop using the child context
	go func() {
		for {
			msg, err := c.reader.FetchMessage(childCtx)
			if err != nil {
				select {
				case errChan <- err:
				case <-childCtx.Done():
				}
				return
			}
			select {
			case msgChan <- msg:
			case <-childCtx.Done():
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errChan:
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) {
				return nil
			}
			return err
		case firstMsg := <-msgChan:
			batchMessages := []kafka.Message{firstMsg}
			batchEvents := make([]domain.RewardEvent, 0, batchSize)

			var ev domain.RewardEvent
			if err := json.Unmarshal(firstMsg.Value, &ev); err == nil {
				batchEvents = append(batchEvents, ev)
			}

			// Collect subsequent messages with a non-disruptive timer timeout
			limitTimer := time.NewTimer(timeout)
			outOfLoop := false
			for len(batchMessages) < batchSize && !outOfLoop {
				// Priority check: Ensure we exit immediately if timeout or context is cancelled
				select {
				case <-limitTimer.C:
					outOfLoop = true
					continue
				case <-ctx.Done():
					outOfLoop = true
					continue
				default:
				}

				select {
				case nextMsg := <-msgChan:
					batchMessages = append(batchMessages, nextMsg)
					var ev domain.RewardEvent
					if err := json.Unmarshal(nextMsg.Value, &ev); err == nil {
						batchEvents = append(batchEvents, ev)
					}
				case <-limitTimer.C:
					outOfLoop = true
				case <-ctx.Done():
					outOfLoop = true
				}
			}
			if !limitTimer.Stop() {
				select {
				case <-limitTimer.C:
				default:
				}
			}

			if len(batchEvents) > 0 {
				err := handler(ctx, batchEvents)
				if err != nil {
					return err
				}
			}

			if err := c.reader.CommitMessages(ctx, batchMessages...); err != nil {
				return err
			}
		}
	}
}

func (c *kafkaConsumer) Close() error {
	return c.reader.Close()
}
