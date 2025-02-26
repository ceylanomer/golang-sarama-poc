# Kafka Consumer with Sarama - Go Application

This project is a Go application that demonstrates how to consume Kafka messages using the Sarama library. It's containerized with Docker and includes configuration for both Kafka and the consumer application.

## About the Project

This application is designed as a flexible Kafka consumer that can process messages from multiple topics with dedicated handlers. It uses:

- Sarama library for Kafka integration
- Zap for structured logging
- Viper for configuration management
- Docker and Docker Compose for containerization and local development

The consumer connects to Kafka, registers handlers for specific topics, and processes messages according to the business logic in each handler.

## Requirements

- [Docker](https://www.docker.com/get-started)
- [Docker Compose](https://docs.docker.com/compose/install/)
- [Go](https://golang.org/dl/) (1.24+ for local development)
- [Kafka](https://kafka.apache.org/) (provided via Docker Compose)

## Installation and Running

### Running with Docker Compose

1. Clone the project:
   ```bash
   git clone https://gitlab.tygoinfra.com/technology/tgo/local-commerce/logistics/store/poc/golang-sarama-poc.git
   cd golang-sarama-poc
   ```

2. Build and run the application with Kafka using Docker Compose:
   ```bash
   docker-compose up
   ```

   This will start:
   - Zookeeper
   - Kafka broker
   - Kafka topics setup
   - Kafka UI (accessible at http://localhost:8080)
   - The consumer application

### For Local Development

1. Start Kafka infrastructure:
   ```bash
   docker-compose up zookeeper kafka kafka-setup kafka-ui
   ```

2. Install dependencies:
   ```bash
   go mod download
   ```

3. Run the application:
   ```bash
   go run main.go
   ```

## Configuration

The application is configured using YAML files and environment variables:

### Kafka Configuration
- `kafka.brokers`: List of Kafka brokers to connect to
- `kafka.group_id`: Consumer group ID
- `kafka.topics`: Map of topics to consume from
- `kafka.init_offset`: Initial offset strategy (`oldest` or `newest`)

### Logger Configuration
- `logger.level`: Log level (info, debug, warn, error)

### Environment Variables
All configuration options can be overridden with environment variables using the prefix `APP_`:
- `APP_KAFKA_BROKERS`
- `APP_KAFKA_GROUP_ID`
- etc.

## Project Structure

```
.
├── config/                 # Configuration files
│   └── config.yml          # Main application configuration
├── consumer/               # Kafka consumer implementation
│   └── consumer.go         # Consumer logic
├── handlers/               # Message handlers
│   └── handlers.go         # Business logic for processing messages
├── models/                 # Data models
│   └── product.go          # Product event model
├── pkg/                    # Shared packages
│   ├── config/             # Configuration loading
│   └── log/                # Logging setup
├── Dockerfile              # Docker image configuration
├── docker-compose.yml      # Docker Compose configuration for all services
├── go.mod                  # Go module dependencies
├── go.sum                  # Go module checksums
├── main.go                 # Main application entry point
└── README.md               # Project documentation
```

## How It Works

The application follows these steps:

1. Load configuration from files and environment variables
2. Initialize the Kafka consumer with the specified broker(s)
3. Register handlers for each configured topic
4. Start consuming messages
5. Process messages with the appropriate handler based on the topic
6. Handle shutdown gracefully when receiving termination signals

## Kafka Topics

The application is configured to handle these topics:

- `sarama-topic-1`: Product update events
- `sarama-topic-2`: Product delete events

## License

[Add your license information here] 