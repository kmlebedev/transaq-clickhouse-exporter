package main

import (
	"slices"
	"testing"
	"time"
)

func TestNearestFutures(t *testing.T) {
	forts := block{
		Columns: []string{"SECID", "ASSETCODE", "LASTTRADEDATE"},
		Data: [][]any{
			{"MXU6", "MIX", "2026-09-17"}, // истёк
			{"MXZ6", "MIX", "2026-12-17"},
			{"MXH7", "MIX", "2027-03-18"},
			{"MXM7", "MIX", "2027-06-17"},
			{"RIH7", "RTS", "2027-03-18"},
			{"RIZ6", "RTS", "2026-12-17"},
			{"RIF7", "RTS", "2027-01-21"}, // месячный, не квартальный
			{"MMZ6", "MXI", "2026-12-17"},
			{"MMH7", "MXI", "2027-03-18"},
			{"MXZ6MXH7", "MIX", "2026-12-17"}, // календарный спред
			{"SiZ6", "Si", "2026-12-17"},      // чужой актив
			{"IMOEXF", "IMOEX", "2100-01-01"}, // вечный задаётся отдельно
		},
	}
	today := time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC)
	got, err := nearestFutures(forts, []string{"MIX", "RTS", "MXI"}, today, 2)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"MXZ6", "MXH7", "RIZ6", "RIH7", "MMZ6", "MMH7"}
	if !slices.Equal(got, want) {
		t.Fatalf("nearestFutures = %v, want %v", got, want)
	}
}

func TestNearestFuturesExpiryDayStillTrades(t *testing.T) {
	forts := block{
		Columns: []string{"SECID", "ASSETCODE", "LASTTRADEDATE"},
		Data: [][]any{
			{"MXZ6", "MIX", "2026-12-17"},
			{"MXH7", "MIX", "2027-03-18"},
			{"MXM7", "MIX", "2027-06-17"},
		},
	}
	today := time.Date(2026, 12, 17, 0, 0, 0, 0, time.UTC)
	got, err := nearestFutures(forts, []string{"MIX"}, today, 2)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"MXZ6", "MXH7"}; !slices.Equal(got, want) {
		t.Fatalf("nearestFutures = %v, want %v", got, want)
	}
}

func TestNearestFuturesMissingAsset(t *testing.T) {
	forts := block{
		Columns: []string{"SECID", "ASSETCODE", "LASTTRADEDATE"},
		Data:    [][]any{{"MXZ6", "MIX", "2026-12-17"}},
	}
	today := time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC)
	if _, err := nearestFutures(forts, []string{"MIX", "RTS"}, today, 1); err == nil {
		t.Fatal("want error when an asset has no live contract")
	}
}
