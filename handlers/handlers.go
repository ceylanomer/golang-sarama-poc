package handlers

import (
	"context"
	"encoding/json"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"golang-sarama-poc/models"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

// Topic1Handler processes product update events
func Topic1Handler(message *sarama.ConsumerMessage) error {
	//tracer := otel.Tracer("kafka-consumer")
	//var parentTraceID string
	//for _, h := range message.Headers {
	//	if string(h.Key) == "trace_id" {
	//		parentTraceID = string(h.Value)
	//		break
	//	}
	//}
	//ctx := context.Background()
	//opts := []trace.SpanStartOption{}
	//if parentTraceID != "" {
	//	opts = append(opts, trace.WithAttributes(attribute.String("parent.trace_id", parentTraceID)))
	//}
	//
	//// Yeni bir span oluştur
	//ctx, span := tracer.Start(ctx, "consumeMessage", opts...)
	//span.SetAttributes(attribute.String("kafka.topic", message.Topic))
	//defer span.End()
	tracer := otel.Tracer("kafka-consumer")
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

	// Parse the message value as a ProductEvent
	var product models.ProductEvent
	if err := json.Unmarshal(message.Value, &product); err != nil {
		zap.L().Error("Failed to unmarshal product update event",
			zap.Error(err),
			zap.ByteString("message", message.Value))
		return err
	}

	// Log the product update event
	zap.L().Info("Product update event received",
		zap.String("id", product.ID),
		zap.String("name", product.Name),
		zap.String("topic", message.Topic),
		zap.Int32("partition", message.Partition),
		zap.Int64("offset", message.Offset))

	// Process the product update event
	// ... your business logic here ...

	return nil
}

// Topic2Handler processes product delete events
func Topic2Handler(message *sarama.ConsumerMessage) error {
	// Parse the message value as a ProductEvent
	var product models.ProductEvent
	if err := json.Unmarshal(message.Value, &product); err != nil {
		zap.L().Error("Failed to unmarshal product delete event",
			zap.Error(err),
			zap.ByteString("message", message.Value))
		return err
	}

	// Log the product delete event
	zap.L().Info("Product delete event received",
		zap.String("id", product.ID),
		zap.String("name", product.Name),
		zap.String("topic", message.Topic),
		zap.Int32("partition", message.Partition),
		zap.Int64("offset", message.Offset))

	// Process the product delete event
	// ... your business logic here ...

	return nil
}
