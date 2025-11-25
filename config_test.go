package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigFile(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.toml")
	content := []byte(`
nsqd_address = "file:4150"
nsqd_http_address = "file:4151"
http_address = ":8081"
bearer_token = "file-token"
`)

	if err := os.WriteFile(configPath, content, 0o600); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, loaded, err := loadConfigFile(configPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !loaded {
		t.Fatalf("expected config to be loaded")
	}

	if cfg.NSQDAddress != "file:4150" || cfg.NSQDHTTPAddress != "file:4151" || cfg.HTTPAddress != ":8081" || cfg.BearerToken != "file-token" {
		t.Fatalf("unexpected config loaded: %+v", cfg)
	}

	_, loaded, err = loadConfigFile(filepath.Join(tmpDir, "missing.toml"))
	if err != nil {
		t.Fatalf("unexpected error for missing file: %v", err)
	}

	if loaded {
		t.Fatalf("expected missing config file to be skipped")
	}

	malformedPath := filepath.Join(tmpDir, "malformed.toml")
	malformedContent := []byte(`nsqd_address = "file:4150"
[[`)
	if err := os.WriteFile(malformedPath, malformedContent, 0o600); err != nil {
		t.Fatalf("failed to write malformed config file: %v", err)
	}

	_, loaded, err = loadConfigFile(malformedPath)
	if err == nil {
		t.Fatalf("expected error for malformed config file")
	}

	if loaded {
		t.Fatalf("expected malformed config file to be treated as not loaded")
	}
}

func TestConfigPrecedence(t *testing.T) {
	cfg := AppConfig{}

	mergeConfig(&cfg, AppConfig{
		NSQDAddress:     "file:4150",
		NSQDHTTPAddress: "file:4151",
		HTTPAddress:     "file:8081",
		BearerToken:     "file-token",
	})

	t.Setenv("NSQ_HTTP_FACADE_NSQD_ADDRESS", "env:4150")
	t.Setenv("NSQ_HTTP_FACADE_BEARER_TOKEN", "env-token")
	applyEnvOverrides(&cfg)

	visited := map[string]bool{
		"nsqd-http-address": true,
		"bearer-token":      true,
	}

	originalNSQDHTTPAddr := *nsqdHTTPAddr
	originalBearerToken := *bearerToken
	defer func() {
		*nsqdHTTPAddr = originalNSQDHTTPAddr
		*bearerToken = originalBearerToken
	}()

	*nsqdHTTPAddr = "cli:4151"
	*bearerToken = "cli-token"

	applyCLIOverrides(&cfg, visited)

	if cfg.NSQDAddress != "env:4150" {
		t.Fatalf("expected env override to win for NSQDAddress, got %s", cfg.NSQDAddress)
	}

	if cfg.NSQDHTTPAddress != "cli:4151" {
		t.Fatalf("expected CLI override to win for NSQDHTTPAddress, got %s", cfg.NSQDHTTPAddress)
	}

	if cfg.BearerToken != "cli-token" {
		t.Fatalf("expected CLI override to win for BearerToken, got %s", cfg.BearerToken)
	}

	if cfg.HTTPAddress != "file:8081" {
		t.Fatalf("expected HTTPAddress to remain configured, got %s", cfg.HTTPAddress)
	}
}

func TestValidateConfig(t *testing.T) {
	err := validateConfig(AppConfig{})
	if err == nil || err.Error() != "missing required configuration values: nsqd_address, nsqd_http_address, http_address, bearer_token" {
		t.Fatalf("expected aggregated missing parameters error, got %v", err)
	}

	cfg := AppConfig{
		NSQDAddress:     "n:4150",
		NSQDHTTPAddress: "n:4151",
		HTTPAddress:     "n:8080",
		BearerToken:     "token",
	}

	if err := validateConfig(cfg); err != nil {
		t.Fatalf("expected valid config, got %v", err)
	}
}
