package config

import (
	"os"
	"strconv"
	"strings"
)

type ServerConfig struct {
	Port int
}

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

type KafkaConfig struct {
	Brokers            []string
	PaymentEventsTopic string
	FraudEventsTopic   string
	FraudGroupID       string
}

type Config struct {
	Server ServerConfig
	Redis  RedisConfig
	Kafka  KafkaConfig
}

func Load() *Config {
	port, _ := strconv.Atoi(getEnv("PORT", "8003"))
	redisDB, _ := strconv.Atoi(getEnv("REDIS_DB", "0"))

	kafkaBrokersStr := getEnv("KAFKA_BROKERS", "localhost:9092")
	brokers := strings.Split(kafkaBrokersStr, ",")

	return &Config{
		Server: ServerConfig{
			Port: port,
		},
		Redis: RedisConfig{
			Addr:     getEnv("REDIS_HOST", "localhost:6379"),
			Password: getEnv("REDIS_PASSWORD", ""),
			DB:       redisDB,
		},
		Kafka: KafkaConfig{
			Brokers:            brokers,
			PaymentEventsTopic: getEnv("KAFKA_PAYMENT_EVENTS_TOPIC", "payment-events"),
			FraudEventsTopic:   getEnv("KAFKA_FRAUD_EVENTS_TOPIC", "fraud-events"),
			FraudGroupID:       getEnv("KAFKA_FRAUD_GROUP_ID", "fraud-service-group"),
		},
	}
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return defaultVal
}
