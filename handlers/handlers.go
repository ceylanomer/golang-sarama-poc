package handlers

import (
	"context"
	"encoding/json"
	"golang-sarama-poc/models"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

// Topic1Handler processes product update events
func Topic1Handler(message *sarama.ConsumerMessage) error {
	// Create context for OpenTelemetry
	tracer := otel.Tracer("kafka-consumer")
	carrier := propagation.MapCarrier{}
	for _, h := range message.Headers {
		carrier[string(h.Key)] = string(h.Value)
	}

	propagator := otel.GetTextMapPropagator()
	ctx := propagator.Extract(context.Background(), carrier)

	// Create a new span and link it to the parent trace ID
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

	return nil
}

// Topic2Handler processes product delete events
func Topic2Handler(message *sarama.ConsumerMessage) error {
	// Create context for OpenTelemetry
	tracer := otel.Tracer("kafka-consumer")
	carrier := propagation.MapCarrier{}
	for _, h := range message.Headers {
		carrier[string(h.Key)] = string(h.Value)
	}

	propagator := otel.GetTextMapPropagator()
	ctx := propagator.Extract(context.Background(), carrier)

	// Create a new span and link it to the parent trace ID
	ctx, span := tracer.Start(ctx, "consumeMessage")
	span.SetAttributes(attribute.String("kafka.topic", message.Topic))
	defer span.End()

	// Parse the message value as a ProductEvent
	var product models.ProductEvent
	if err := json.Unmarshal(message.Value, &product); err != nil {
		span.RecordError(err)
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

	// TODO: Implement product deletion business logic here
	// This is where you would handle the actual deletion of the product
	// from your data store or perform any other necessary operations

	return nil
}
