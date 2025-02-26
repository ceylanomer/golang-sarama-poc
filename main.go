package main

import (
	"encoding/json"
	"golang-sarama-poc/models"
	"golang-sarama-poc/pkg/tracer"
	"os"
	"os/signal"
	"syscall"

	"github.com/gofiber/fiber/v2"

	"golang-sarama-poc/handlers"
	"golang-sarama-poc/pkg/config"
	"golang-sarama-poc/pkg/consumer"
	_ "golang-sarama-poc/pkg/log"
	"golang-sarama-poc/pkg/producer"

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

	app := fiber.New()

	_ = tracer.InitTracer(cfg.Jaeger)
	// Create producer
	kafkaProducer, err := producer.NewKafkaProducer(cfg.Kafka.Brokers)
	if err != nil {
		zap.L().Fatal("Failed to create producer", zap.Error(err))
	}
	defer kafkaProducer.Close()

	app.Post("/produce", func(c *fiber.Ctx) error {

		event := models.ProductEvent{
			ID:   "1",
			Name: "Product 2",
		}
		jsonData, err := json.Marshal(event)
		if err != nil {
			return c.Status(500).SendString("JSON marshal hatası")
		}

		// Kafka'ya mesaj gönder
		err = kafkaProducer.SendMessage("sarama-topic-1", "1", jsonData)
		if err != nil {
			return c.Status(500).SendString("Kafka'ya mesaj gönderilemedi")
		}
		return c.SendString("Mesaj gönderildi!")
	})

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

	app.Listen(":8081")
	// Signal handling for graceful shutdown
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm

	zap.L().Info("Shutting down...")
	kafkaConsumer.Stop()
}
