package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

// Config holds all configuration for the application
type Config struct {
	Kafka  KafkaConfig
	Logger LoggerConfig
}

// KafkaConfig holds Kafka-specific configuration
type KafkaConfig struct {
	Brokers    []string
	GroupID    string
	Topics     map[string]string
	InitOffset string
}

// LoggerConfig holds logger-specific configuration
type LoggerConfig struct {
	Level string
}

// Load loads the configuration from config files and environment variables
func Load() (*Config, error) {
	v := viper.New()

	// Set default configuration file paths
	v.SetConfigName("config")    // name of config file (without extension)
	v.SetConfigType("yaml")      // YAML format
	v.AddConfigPath("./config/") // look for config in the config directory
	v.AddConfigPath(".")         // optionally look for config in the working directory

	// Set up environment variable support
	v.SetEnvPrefix("APP")                              // prefix for environment variables
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_")) // replace dots with underscores in env vars
	v.AutomaticEnv()                                   // read in environment variables that match

	// Set defaults
	v.SetDefault("kafka.init_offset", "oldest")
	v.SetDefault("logger.level", "info")

	// Read the config file
	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// Config file not found
			return nil, fmt.Errorf("config file not found: %w", err)
		}
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Create config struct
	var config Config
	config.Kafka = KafkaConfig{
		Brokers:    v.GetStringSlice("kafka.brokers"),
		GroupID:    v.GetString("kafka.group_id"),
		Topics:     getTopicsMap(v),
		InitOffset: v.GetString("kafka.init_offset"),
	}
	config.Logger = LoggerConfig{
		Level: v.GetString("logger.level"),
	}

	// Validate config
	if err := validateConfig(&config); err != nil {
		return nil, err
	}

	return &config, nil
}

// getTopicsMap extracts the topics map from viper
func getTopicsMap(v *viper.Viper) map[string]string {
	topics := make(map[string]string)
	topicsMap := v.GetStringMap("kafka.topics")

	for key, value := range topicsMap {
		if strValue, ok := value.(string); ok {
			topics[key] = strValue
		} else {
			// If not a string, convert to string
			topics[key] = fmt.Sprintf("%v", value)
		}
	}

	return topics
}

// validateConfig validates the loaded configuration
func validateConfig(config *Config) error {
	if len(config.Kafka.Brokers) == 0 {
		return fmt.Errorf("no Kafka brokers specified")
	}
	if config.Kafka.GroupID == "" {
		return fmt.Errorf("Kafka group ID is required")
	}
	if len(config.Kafka.Topics) == 0 {
		return fmt.Errorf("no Kafka topics specified")
	}

	// Validate init_offset
	if config.Kafka.InitOffset != "" &&
		config.Kafka.InitOffset != "oldest" &&
		config.Kafka.InitOffset != "newest" {
		return fmt.Errorf("invalid init_offset value: %s (must be 'oldest' or 'newest')",
			config.Kafka.InitOffset)
	}

	return nil
}
