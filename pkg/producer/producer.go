package producer

import (
	"context"
	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
)

// KafkaProducer manages Kafka producer operations
type KafkaProducer struct {
	producer sarama.SyncProducer
	brokers  []string
}

// NewKafkaProducer creates a new KafkaProducer
func NewKafkaProducer(brokers []string) (*KafkaProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	return &KafkaProducer{
		producer: producer,
		brokers:  brokers,
	}, nil
}

// SendMessage sends a message to specified topic
func (p *KafkaProducer) SendMessage(topic string, key string, value []byte) error {

	tracer := otel.Tracer("kafka-producer")

	// OpenTelemetry için context oluştur
	ctx, span := tracer.Start(context.Background(), "produceMessage")
	defer span.End()

	// OpenTelemetry propagator ile trace bilgilerini header’a koy
	propagator := otel.GetTextMapPropagator()
	carrier := propagation.MapCarrier{}
	propagator.Inject(ctx, carrier)

	// Kafka mesajına traceparent ekle
	var headers []sarama.RecordHeader
	for k, v := range carrier {
		headers = append(headers, sarama.RecordHeader{
			Key:   []byte(k),
			Value: []byte(v),
		})
	}

	msg := &sarama.ProducerMessage{
		Topic:   topic,
		Value:   sarama.ByteEncoder(value),
		Headers: headers,
	}

	if key != "" {
		msg.Key = sarama.StringEncoder(key)
	}

	partition, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		span.RecordError(err)
		zap.L().Error("Failed to send message",
			zap.String("topic", topic),
			zap.Error(err))
		return err
	}

	zap.L().Debug("Message sent successfully",
		zap.String("topic", topic),
		zap.Int32("partition", partition),
		zap.Int64("offset", offset))

	return nil
}

// Close closes the producer
func (p *KafkaProducer) Close() error {
	if err := p.producer.Close(); err != nil {
		zap.L().Error("Failed to close producer", zap.Error(err))
		return err
	}
	return nil
}
