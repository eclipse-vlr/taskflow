package config

import (
	"fmt"
	"net"
	"os"
	"strconv"
)

type Config struct {
	HTTPAddr     string
	TaskCapacity int
}

func Load() (Config, error) {
	cfg := Config{HTTPAddr: "127.0.0.1:8080", TaskCapacity: 1000}
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
	if value, ok := os.LookupEnv("TASKFLOW_TASK_CAPACITY"); ok {
		capacity, err := strconv.Atoi(value)
		if err != nil || capacity < 1 || capacity > 100000 {
			return Config{}, fmt.Errorf("TASKFLOW_TASK_CAPACITY must be from 1 to 100000")
		}
		cfg.TaskCapacity = capacity
	}
	return cfg, nil
}
