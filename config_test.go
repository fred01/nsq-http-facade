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
}

func TestConfigPrecedence(t *testing.T) {
	cfg := defaultAppConfig()

	mergeConfig(&cfg, AppConfig{
		NSQDAddress: "file:4150",
		BearerToken: "file-token",
	})

	t.Setenv("NSQ_HTTP_FACADE_NSQD_ADDRESS", "env:4150")
	t.Setenv("NSQ_HTTP_FACADE_BEARER_TOKEN", "env-token")
	applyEnvOverrides(&cfg)

	visited := map[string]bool{
		"nsqd-http-address": true,
		"bearer-token":      true,
	}

	originalHTTPAddr := *nsqdHTTPAddr
	originalBearerToken := *bearerToken
	defer func() {
		*nsqdHTTPAddr = originalHTTPAddr
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

	if cfg.HTTPAddress != defaultAppConfig().HTTPAddress {
		t.Fatalf("expected HTTPAddress to remain default, got %s", cfg.HTTPAddress)
	}
}
