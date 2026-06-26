package config

import (
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server   ServerConfig   `yaml:"server"`
	Database DatabaseConfig `yaml:"database"`
	Redis    RedisConfig    `yaml:"redis"`
	Kafka    KafkaConfig    `yaml:"kafka"`
}

type ServerConfig struct {
	Port int `yaml:"port"`
}

type DatabaseConfig struct {
	DSN string `yaml:"dsn"`
}

type RedisConfig struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

type KafkaConfig struct {
	Brokers  []string `yaml:"brokers"`
	Topic    string   `yaml:"topic"`
	DLQTopic string   `yaml:"dlq_topic"`
	GroupID  string   `yaml:"group_id"`
}

// LoadConfig loads configuration from config.yaml in the specified path or default locations.
func LoadConfig(configPath string) (*Config, error) {
	if configPath == "" {
		// Fallback defaults
		configPath = "config/config.yaml"
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		// Try to look up one level if running from cmd subdirectories
		fallbackPath := filepath.Join("..", "..", configPath)
		data, err = os.ReadFile(fallbackPath)
		if err != nil {
			fallbackPath = filepath.Join("..", configPath)
			data, err = os.ReadFile(fallbackPath)
			if err != nil {
				return nil, err
			}
		}
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	// Environment variable overrides (convenient for Docker environments)
	if dsn := os.Getenv("DATABASE_DSN"); dsn != "" {
		cfg.Database.DSN = dsn
	}
	if redisAddr := os.Getenv("REDIS_ADDR"); redisAddr != "" {
		cfg.Redis.Addr = redisAddr
	}
	if kafkaBrokers := os.Getenv("KAFKA_BROKERS"); kafkaBrokers != "" {
		cfg.Kafka.Brokers = strings.Split(kafkaBrokers, ",")
	}

	return &cfg, nil
}
