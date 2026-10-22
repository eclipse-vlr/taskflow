package config_test

import (
	"testing"

	"taskflow/internal/config"
)

func TestLoad(t *testing.T) {
	for _, tt := range []struct {
		name, address, capacity string
		valid                   bool
	}{
		{"valid", "127.0.0.1:8080", "1000", true},
		{"ipv6", "[::1]:8081", "1", true},
		{"bad address", "localhost", "1", false},
		{"empty address", "", "1", false},
		{"bad port", "localhost:abc", "1", false},
		{"zero port", "localhost:0", "1", false},
		{"large port", "localhost:65536", "1", false},
		{"zero capacity", "localhost:8080", "0", false},
		{"large capacity", "localhost:8080", "100001", false},
		{"bad capacity", "localhost:8080", "many", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("TASKFLOW_HTTP_ADDR", tt.address)
			t.Setenv("TASKFLOW_TASK_CAPACITY", tt.capacity)
			cfg, err := config.Load()
			if (err == nil) != tt.valid {
				t.Fatalf("config = %+v, error = %v, valid = %v", cfg, err, tt.valid)
			}
			if tt.valid && cfg.HTTPAddr != tt.address {
				t.Fatalf("address = %q", cfg.HTTPAddr)
			}
		})
	}
}
