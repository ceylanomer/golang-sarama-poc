package consumer

import (
	"context"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"sync"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

// MessageHandler is a function type that processes a Kafka message
type MessageHandler func(message *sarama.ConsumerMessage) error

// KafkaConsumer manages Kafka consumer operations
type KafkaConsumer struct {
	brokers  []string
	groupID  string
	client   sarama.ConsumerGroup
	handlers map[string]MessageHandler
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// NewKafkaConsumer creates a new KafkaConsumer
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

// RegisterHandler registers a handler for a specific topic
func (c *KafkaConsumer) RegisterHandler(topic string, handler MessageHandler) {
	c.handlers[topic] = handler
}

// Start begins consuming messages and listening for them
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

// Stop shuts down the consumer
func (c *KafkaConsumer) Stop() {
	c.cancel()
	c.wg.Wait()
	c.client.Close()
}

// consumerGroupHandler implements the sarama.ConsumerGroupHandler interface
type consumerGroupHandler struct {
	handlers map[string]MessageHandler
}

func (h *consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	tracer := otel.Tracer("kafka-consumer")

	for message := range claim.Messages() {
		carrier := propagation.MapCarrier{}
		for _, h := range message.Headers {
			carrier[string(h.Key)] = string(h.Value)
		}

		propagator := otel.GetTextMapPropagator()
		ctx := propagator.Extract(context.Background(), carrier)

		// Yeni span oluştur ve parent trace ID'yi bağla
		ctx, span := tracer.Start(ctx, "consumeMessage")
		span.SetAttributes(attribute.String("kafka.topic", message.Topic))
		defer span.End()

		if handler, ok := h.handlers[message.Topic]; ok {
			if err := handler(message); err != nil {
				zap.L().Error("Error handling message: %v", zap.Error(err))
			}
		}
		session.MarkMessage(message, "")
	}
	return nil
}
