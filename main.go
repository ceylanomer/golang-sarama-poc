package main

import (
	"os"
	"os/signal"
	"syscall"

	"golang-sarama-poc/consumer"
	"golang-sarama-poc/handlers"
	"golang-sarama-poc/pkg/config"
	_ "golang-sarama-poc/pkg/log"

	"go.uber.org/zap"
)

func main() {
	// Load configuration
	cfg, err := config.Load()
	defer zap.L().Sync()
	if err != nil {
		// Use standard log until our logger is initialized
		panic("Failed to load configuration: " + err.Error())
	}

	// Create consumer
	kafkaConsumer, err := consumer.NewKafkaConsumer(cfg.Kafka.Brokers, cfg.Kafka.GroupID, cfg.Kafka.InitOffset)
	if err != nil {
		zap.L().Fatal("Failed to create consumer", zap.Error(err))
	}

	// Register handlers for all topics in config
	for topic := range cfg.Kafka.Topics {
		switch topic {
		case "sarama-topic-1":
			zap.L().Info("Registering handler for product update events", zap.String("topic", topic))
			kafkaConsumer.RegisterHandler(topic, handlers.Topic1Handler)
		case "sarama-topic-2":
			zap.L().Info("Registering handler for product delete events", zap.String("topic", topic))
			kafkaConsumer.RegisterHandler(topic, handlers.Topic2Handler)
		default:
			zap.L().Warn("No handler registered for topic", zap.String("topic", topic))
		}
	}

	// Start the consumer
	if err := kafkaConsumer.Start(); err != nil {
		zap.L().Fatal("Failed to start consumer", zap.Error(err))
	}

	zap.L().Info("Consumer started successfully")

	// Signal handling for graceful shutdown
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm

	zap.L().Info("Shutting down...")
	kafkaConsumer.Stop()
}
