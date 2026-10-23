package config_test

import (
	"strings"
	"testing"
	"time"

	"taskflow/internal/config"
)

func TestLoad(t *testing.T) {
	for _, tt := range []struct {
		name, address, maxConns string
		valid                   bool
	}{
		{"valid", "127.0.0.1:8080", "10", true},
		{"ipv6", "[::1]:8081", "1", true},
		{"bad address", "localhost", "1", false},
		{"empty address", "", "1", false},
		{"bad port", "localhost:abc", "1", false},
		{"zero port", "localhost:0", "1", false},
		{"large port", "localhost:65536", "1", false},
		{"zero connections", "localhost:8080", "0", false},
		{"too many connections", "localhost:8080", "51", false},
		{"bad connections", "localhost:8080", "many", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("TASKFLOW_HTTP_ADDR", tt.address)
			t.Setenv("DATABASE_URL", "postgres://taskflow:secret@localhost/taskflow?sslmode=disable")
			t.Setenv("TASKFLOW_DB_TIMEOUT", "3s")
			t.Setenv("TASKFLOW_DB_MAX_CONNS", tt.maxConns)
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

func TestDatabaseSettings(t *testing.T) {
	for _, tt := range []struct {
		name, databaseURL, timeout string
		valid                      bool
	}{
		{"valid", "postgresql://user:secret@localhost/taskflow", "1s", true},
		{"minimum timeout", "postgres://localhost/taskflow", "1ms", true},
		{"maximum timeout", "postgres://localhost/taskflow", "5s", true},
		{"missing URL", "", "3s", false},
		{"wrong scheme", "http://user:secret@localhost/taskflow", "3s", false},
		{"bad URL", "postgres://user:secret@%/taskflow", "3s", false},
		{"missing database", "postgres://localhost/", "3s", false},
		{"missing host", "postgres:///taskflow", "3s", false},
		{"zero timeout", "postgres://localhost/taskflow", "0s", false},
		{"large timeout", "postgres://localhost/taskflow", "6s", false},
		{"bad timeout", "postgres://localhost/taskflow", "later", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("TASKFLOW_HTTP_ADDR", "127.0.0.1:8080")
			t.Setenv("TASKFLOW_DB_MAX_CONNS", "10")
			t.Setenv("DATABASE_URL", tt.databaseURL)
			t.Setenv("TASKFLOW_DB_TIMEOUT", tt.timeout)
			cfg, err := config.Load()
			if (err == nil) != tt.valid {
				t.Fatalf("error = %v, valid = %v", err, tt.valid)
			}
			if err != nil && strings.Contains(err.Error(), "secret") {
				t.Fatalf("error leaked password: %v", err)
			}
			if tt.valid {
				want, _ := time.ParseDuration(tt.timeout)
				if cfg.DatabaseURL != tt.databaseURL || cfg.DBTimeout != want || cfg.DBMaxConns != 10 {
					t.Fatal("configuration differs from supplied settings")
				}
			}
		})
	}
}
