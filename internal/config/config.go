package config

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddr    string
	DatabaseURL string
	DBTimeout   time.Duration
	DBMaxConns  int32
}

func Load() (Config, error) {
	cfg := Config{HTTPAddr: "127.0.0.1:8080", DBTimeout: 3 * time.Second, DBMaxConns: 10}
	if value, ok := os.LookupEnv("TASKFLOW_HTTP_ADDR"); ok {
		cfg.HTTPAddr = value
	}
	_, port, err := net.SplitHostPort(cfg.HTTPAddr)
	if err != nil {
		return Config{}, fmt.Errorf("invalid TASKFLOW_HTTP_ADDR: %w", err)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return Config{}, fmt.Errorf("TASKFLOW_HTTP_ADDR must contain a port from 1 to 65535")
	}
	cfg.DatabaseURL = os.Getenv("DATABASE_URL")
	databaseURL, err := url.Parse(cfg.DatabaseURL)
	if err != nil || (databaseURL.Scheme != "postgres" && databaseURL.Scheme != "postgresql") || databaseURL.Hostname() == "" || databaseURL.Path == "" || databaseURL.Path == "/" {
		return Config{}, fmt.Errorf("DATABASE_URL must be a PostgreSQL URL with host and database name")
	}
	if value, ok := os.LookupEnv("TASKFLOW_DB_TIMEOUT"); ok {
		timeout, err := time.ParseDuration(value)
		if err != nil || timeout < time.Millisecond || timeout > 5*time.Second {
			return Config{}, fmt.Errorf("TASKFLOW_DB_TIMEOUT must be from 1ms to 5s")
		}
		cfg.DBTimeout = timeout
	}
	if value, ok := os.LookupEnv("TASKFLOW_DB_MAX_CONNS"); ok {
		maxConns, err := strconv.ParseInt(value, 10, 32)
		if err != nil || maxConns < 1 || maxConns > 50 {
			return Config{}, fmt.Errorf("TASKFLOW_DB_MAX_CONNS must be from 1 to 50")
		}
		cfg.DBMaxConns = int32(maxConns)
	}
	return cfg, nil
}
