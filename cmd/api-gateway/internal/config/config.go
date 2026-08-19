package config

import (
	"os"
	"strconv"
)

type ServerConfig struct {
	Port int
}

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

type Config struct {
	Server ServerConfig
	Redis  RedisConfig
}

func Load() *Config {
	port, _ := strconv.Atoi(getEnv("PORT", "8000"))
	redisDB, _ := strconv.Atoi(getEnv("REDIS_DB", "0"))

	return &Config{
		Server: ServerConfig{
			Port: port,
		},
		Redis: RedisConfig{
			Addr:     getEnv("REDIS_HOST", "localhost:6379"),
			Password: getEnv("REDIS_PASSWORD", ""),
			DB:       redisDB,
		},
	}
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return defaultVal
}
