package producer

import (
	"context"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
)

// KafkaProducer manages Kafka producer operations and maintains connection to Kafka brokers
type KafkaProducer struct {
	producer sarama.SyncProducer // Synchronous producer instance
	brokers  []string            // List of Kafka broker addresses
}

// NewKafkaProducer creates a new KafkaProducer with the specified broker configuration
func NewKafkaProducer(brokers []string) (*KafkaProducer, error) {
	// Configure Kafka producer settings
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all replicas to acknowledge
	config.Producer.Retry.Max = 5                    // Retry up to 5 times on failure
	config.Producer.Return.Successes = true          // Required for SyncProducer

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	return &KafkaProducer{
		producer: producer,
		brokers:  brokers,
	}, nil
}

// SendMessage sends a message to specified topic with OpenTelemetry tracing
func (p *KafkaProducer) SendMessage(topic string, key string, value []byte) error {
	// Initialize OpenTelemetry tracer
	tracer := otel.Tracer("kafka-producer")

	// Create context for OpenTelemetry
	ctx, span := tracer.Start(context.Background(), "produceMessage")
	defer span.End()

	// Put trace information into header with OpenTelemetry propagator
	propagator := otel.GetTextMapPropagator()
	carrier := propagation.MapCarrier{}
	propagator.Inject(ctx, carrier)

	// Add traceparent to Kafka message headers for distributed tracing
	var headers []sarama.RecordHeader
	for k, v := range carrier {
		headers = append(headers, sarama.RecordHeader{
			Key:   []byte(k),
			Value: []byte(v),
		})
	}

	// Prepare Kafka message with headers and payload
	msg := &sarama.ProducerMessage{
		Topic:   topic,
		Value:   sarama.ByteEncoder(value),
		Headers: headers,
	}

	// Add message key if provided
	if key != "" {
		msg.Key = sarama.StringEncoder(key)
	}

	// Send message to Kafka and get partition and offset
	partition, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		span.RecordError(err)
		zap.L().Error("Failed to send message",
			zap.String("topic", topic),
			zap.Error(err))
		return err
	}

	// Log successful message delivery
	zap.L().Debug("Message sent successfully",
		zap.String("topic", topic),
		zap.Int32("partition", partition),
		zap.Int64("offset", offset))

	return nil
}

// Close gracefully shuts down the Kafka producer
func (p *KafkaProducer) Close() error {
	if err := p.producer.Close(); err != nil {
		zap.L().Error("Failed to close producer", zap.Error(err))
		return err
	}
	return nil
}
