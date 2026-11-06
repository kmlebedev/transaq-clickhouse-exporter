package main

import (
	"strings"
	"testing"
)

func setTransaqEnv(t *testing.T) {
	t.Helper()
	t.Setenv("TC_LOGIN", "TESTLOGIN")
	t.Setenv("TC_PASSWORD", "TESTPASS")
	t.Setenv("TC_HOST", "127.0.0.1")
	t.Setenv("TC_PORT", "3900")
}

func TestNewProviderDefaultsToNative(t *testing.T) {
	setTransaqEnv(t)
	provider, err := NewProvider("")
	if err != nil {
		t.Fatal(err)
	}
	if provider.Name() != backendNative {
		t.Fatalf("provider = %q, want %q", provider.Name(), backendNative)
	}
}

func TestNewProviderGRPC(t *testing.T) {
	setTransaqEnv(t)
	provider, err := NewProvider("grpc")
	if err != nil {
		t.Fatal(err)
	}
	if provider.Name() != backendGRPC {
		t.Fatalf("provider = %q, want %q", provider.Name(), backendGRPC)
	}
}

func TestNewProviderUnknown(t *testing.T) {
	_, err := NewProvider("unknown-provider")
	if err == nil {
		t.Fatal("неизвестный провайдер не отвергнут")
	}
	if !strings.Contains(err.Error(), "native") || !strings.Contains(err.Error(), "grpc") {
		t.Fatalf("ошибка не перечисляет доступные провайдеры: %v", err)
	}
}

func TestNativeProviderConfigValidation(t *testing.T) {
	t.Setenv("TC_LOGIN", "")
	t.Setenv("TC_PASSWORD", "")
	t.Setenv("TC_HOST", "")
	t.Setenv("TC_PORT", "")
	if _, err := newNativeProvider(); err == nil {
		t.Fatal("native provider принял пустой конфиг")
	}
}

func TestGRPCProviderConfigValidation(t *testing.T) {
	t.Setenv("TC_LOGIN", "")
	t.Setenv("TC_PASSWORD", "")
	t.Setenv("TC_HOST", "")
	t.Setenv("TC_PORT", "")
	if _, err := newGRPCProvider(); err == nil {
		t.Fatal("grpc provider принял пустой конфиг")
	}
}

func TestNativeConfigFromEnv(t *testing.T) {
	setTransaqEnv(t)
	cfg, err := nativeConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Address != "127.0.0.1:3900" || cfg.Login != "TESTLOGIN" || !cfg.MicexRegisters {
		t.Fatalf("config = %+v", cfg)
	}
}
