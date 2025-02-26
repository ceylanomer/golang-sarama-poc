// Package consumer provides Kafka consumer functionality with support for multiple topics and handlers
package consumer

import (
	"context"
	"sync"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

// MessageHandler is a function type that processes a Kafka message
// It takes a ConsumerMessage and returns an error if processing fails
type MessageHandler func(message *sarama.ConsumerMessage) error

// KafkaConsumer manages Kafka consumer operations with support for multiple topics
type KafkaConsumer struct {
	brokers  []string                  // List of Kafka broker addresses
	groupID  string                    // Consumer group ID for this consumer
	client   sarama.ConsumerGroup      // Sarama consumer group client
	handlers map[string]MessageHandler // Map of topic to message handlers
	ctx      context.Context           // Context for controlling the consumer lifecycle
	cancel   context.CancelFunc        // Function to cancel the consumer context
	wg       sync.WaitGroup            // WaitGroup for graceful shutdown
}

// NewKafkaConsumer creates a new KafkaConsumer with the specified configuration
// brokers: List of Kafka broker addresses
// groupID: Consumer group ID
// initOffset: Initial offset configuration ("newest" or "oldest")
func NewKafkaConsumer(brokers []string, groupID string, initOffset string) (*KafkaConsumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin

	// Set initial offset based on config
	if initOffset == "newest" {
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	} else {
		// Default to oldest if not specified or invalid
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	client, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &KafkaConsumer{
		brokers:  brokers,
		groupID:  groupID,
		client:   client,
		handlers: make(map[string]MessageHandler),
		ctx:      ctx,
		cancel:   cancel,
	}, nil
}

// RegisterHandler registers a message handler function for a specific topic
// This must be called before Start() for each topic you want to consume
func (c *KafkaConsumer) RegisterHandler(topic string, handler MessageHandler) {
	c.handlers[topic] = handler
}

// Start begins consuming messages from all registered topics
// It runs in a separate goroutine and continues until Stop() is called
func (c *KafkaConsumer) Start() error {
	if len(c.handlers) == 0 {
		zap.L().Warn("No handlers registered, consumer will not start")
		return nil
	}

	topics := make([]string, 0, len(c.handlers))
	for topic := range c.handlers {
		topics = append(topics, topic)
	}

	zap.L().Info("Starting consumer", zap.Strings("topics", topics), zap.String("group", c.groupID))

	handler := &consumerGroupHandler{
		handlers: c.handlers,
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			if err := c.client.Consume(c.ctx, topics, handler); err != nil {
				zap.L().Error("Error from consumer", zap.Error(err))
			}
			if c.ctx.Err() != nil {
				return
			}
		}
	}()

	return nil
}

// Stop gracefully shuts down the consumer
// It waits for all message processing to complete before returning
func (c *KafkaConsumer) Stop() {
	c.cancel()
	c.wg.Wait()
	c.client.Close()
}

// consumerGroupHandler implements the sarama.ConsumerGroupHandler interface
// It manages the lifecycle of consumer group sessions and message consumption
type consumerGroupHandler struct {
	handlers map[string]MessageHandler
}

// Setup is called when the consumer group session is being set up
func (h *consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is called when the consumer group session is ending
func (h *consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim processes messages for a specific topic partition
// It routes messages to the appropriate handler based on the topic
func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		if handler, ok := h.handlers[message.Topic]; ok {
			if err := handler(message); err != nil {
				zap.L().Error("Error handling message",
					zap.String("topic", message.Topic),
					zap.Int32("partition", message.Partition),
					zap.Int64("offset", message.Offset),
					zap.Error(err))
			}
		}
		session.MarkMessage(message, "")
	}
	return nil
}
