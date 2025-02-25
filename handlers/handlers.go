package handlers

import (
	"encoding/json"
	"golang-sarama-poc/models"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

// Topic1Handler processes product update events
func Topic1Handler(message *sarama.ConsumerMessage) error {
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
