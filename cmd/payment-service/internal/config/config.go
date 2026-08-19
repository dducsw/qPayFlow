package config

import (
	"os"
	"strconv"
	"strings"
)

type ServerConfig struct {
	Port int
}

type PostgresConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
	SSLMode  string
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
}

type Config struct {
	Server   ServerConfig
	Postgres PostgresConfig
	Redis    RedisConfig
	Kafka    KafkaConfig
}

func Load() *Config {
	port, _ := strconv.Atoi(getEnv("PORT", "8001"))
	pgPort, _ := strconv.Atoi(getEnv("POSTGRES_PORT", "5432"))
	redisDB, _ := strconv.Atoi(getEnv("REDIS_DB", "0"))

	kafkaBrokersStr := getEnv("KAFKA_BROKERS", "localhost:9092")
	brokers := strings.Split(kafkaBrokersStr, ",")

	return &Config{
		Server: ServerConfig{
			Port: port,
		},
		Postgres: PostgresConfig{
			Host:     getEnv("POSTGRES_HOST", "localhost"),
			Port:     pgPort,
			User:     getEnv("POSTGRES_USER", "qpayflow"),
			Password: getEnv("POSTGRES_PASSWORD", "qpayflow_secret"),
			DBName:   getEnv("POSTGRES_DB", "qpayflow_db"),
			SSLMode:  getEnv("POSTGRES_SSLMODE", "disable"),
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
		},
	}
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return defaultVal
}
