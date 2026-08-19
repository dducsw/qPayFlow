package config

import (
	"os"
	"strconv"
	"strings"
)

type ServerConfig struct {
	Port int
}

type KafkaConfig struct {
	Brokers             []string
	PaymentEventsTopic  string
	NotificationGroupID string
}

type Config struct {
	Server ServerConfig
	Kafka  KafkaConfig
}

func Load() *Config {
	port, _ := strconv.Atoi(getEnv("PORT", "8004"))

	kafkaBrokersStr := getEnv("KAFKA_BROKERS", "localhost:9092")
	brokers := strings.Split(kafkaBrokersStr, ",")

	return &Config{
		Server: ServerConfig{
			Port: port,
		},
		Kafka: KafkaConfig{
			Brokers:             brokers,
			PaymentEventsTopic:  getEnv("KAFKA_PAYMENT_EVENTS_TOPIC", "payment-events"),
			NotificationGroupID: getEnv("KAFKA_NOTIFICATION_GROUP_ID", "notification-service-group"),
		},
	}
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return defaultVal
}
