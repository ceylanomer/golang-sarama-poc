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
	// Load configuration from environment or config files
	cfg, err := config.Load()
	defer zap.L().Sync()
	if err != nil {
		panic("Failed to load configuration: " + err.Error())
	}

	// Initialize Fiber web framework
	app := fiber.New()

	// Initialize OpenTelemetry tracer with Jaeger configuration
	_ = tracer.InitTracer(cfg.Jaeger)

	// Initialize Kafka producer
	kafkaProducer, err := producer.NewKafkaProducer(cfg.Kafka.Brokers)
	if err != nil {
		zap.L().Fatal("Failed to create producer", zap.Error(err))
	}
	defer kafkaProducer.Close()

	// REST endpoint to produce messages to Kafka
	app.Post("/produce", func(c *fiber.Ctx) error {
		// Create a sample product event
		// TODO: Replace with actual request body parsing
		event := models.ProductEvent{
			ID:   "1",
			Name: "Product 2",
		}
		jsonData, err := json.Marshal(event)
		if err != nil {
			return c.Status(500).SendString("JSON marshal error")
		}

		// Send message to Kafka
		err = kafkaProducer.SendMessage("sarama-topic-1", "1", jsonData)
		if err != nil {
			return c.Status(500).SendString("Failed to send message to Kafka")
		}
		return c.SendString("Message sent successfully!")
	})

	// Initialize Kafka consumer with configuration
	kafkaConsumer, err := consumer.NewKafkaConsumer(cfg.Kafka.Brokers, cfg.Kafka.GroupID, cfg.Kafka.InitOffset)
	if err != nil {
		zap.L().Fatal("Failed to create consumer", zap.Error(err))
	}

	// Register message handlers for each configured Kafka topic
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

	// Start consuming messages from Kafka
	if err := kafkaConsumer.Start(); err != nil {
		zap.L().Fatal("Failed to start consumer", zap.Error(err))
	}

	zap.L().Info("Consumer started successfully")

	// Start the HTTP server in a separate goroutine
	go app.Listen(":8081")

	// Setup graceful shutdown handling
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm

	zap.L().Info("Shutting down...")
	kafkaConsumer.Stop()
}
