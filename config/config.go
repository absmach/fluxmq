package config

import (
	"flag"
	"os"
)

// Config holds all configuration for the broker.
type Config struct {
	Server ServerConfig
	Log    LogConfig
}

// ServerConfig holds server-related configuration.
type ServerConfig struct {
	TCPAddr  string
	HTTPAddr string
}

// LogConfig holds logging configuration.
type LogConfig struct {
	Level  string
	Format string
}

// Load loads configuration from environment variables and flags.
// Flags take precedence over environment variables.
func Load() *Config {
	cfg := &Config{}

	// Defaults / Environment Variables
	defaultTCPAddr := getEnv("MQTT_TCP_ADDR", ":1883")
	defaultHTTPAddr := getEnv("MQTT_HTTP_ADDR", ":8080")
	defaultLogLevel := getEnv("MQTT_LOG_LEVEL", "info")
	defaultLogFormat := getEnv("MQTT_LOG_FORMAT", "text")

	// Flags
	flag.StringVar(&cfg.Server.TCPAddr, "addr", defaultTCPAddr, "MQTT broker TCP listen address")
	flag.StringVar(&cfg.Server.HTTPAddr, "http-addr", defaultHTTPAddr, "HTTP adapter listen address")
	flag.StringVar(&cfg.Log.Level, "log", defaultLogLevel, "Log level (debug, info, warn, error)")
	flag.StringVar(&cfg.Log.Format, "log-format", defaultLogFormat, "Log format (text, json)")

	flag.Parse()

	return cfg
}

// getEnv retrieves the value of the environment variable named by the key.
// It returns the value, which will be the default if the variable is not present.
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
