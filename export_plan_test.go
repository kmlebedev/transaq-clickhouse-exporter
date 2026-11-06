package main

import (
	"testing"
)

func testCatalog() Catalog {
	return Catalog{
		Securities: []Security{
			{SecID: 1, Active: true, SecCode: "SBER", ShortName: "Сбербанк", Board: "TQBR", SecType: "SHARE"},
			{SecID: 2, Active: true, SecCode: "GAZP", ShortName: "Газпром", Board: "TQBR", SecType: "SHARE"},
			{SecID: 3, Active: true, SecCode: "SU26238RMFS9", ShortName: "ОФЗ-ПД 26238", Board: "TQOB", SecType: "BOND"},
			{SecID: 4, Active: true, SecCode: "MXZ6", ShortName: "MIX-12.26", Board: "FUT", SecType: "FUT"},
			{SecID: 5, Active: false, SecCode: "OLD", ShortName: "Погашенная", Board: "TQBR", SecType: "SHARE"},
			{SecID: 0, Active: true, SecCode: "NOSECID", ShortName: "Без secid", Board: "TQBR", SecType: "SHARE"},
			{SecID: 6, Active: true, SecCode: "TOOLONGSECCODE123", ShortName: "Длинный", Board: "TQBR", SecType: "SHARE"},
			{SecID: 7, Active: true, SecCode: "LQDT", ShortName: "Ликвидность", Board: "MCT", SecType: "SHARE"},
		},
		CandleKinds: []CandleKind{
			{ID: 1, Period: 60, Name: "1 мин"},
			{ID: 2, Period: 300, Name: "5 мин"},
			{ID: 7, Period: 86400, Name: "1 день"},
		},
	}
}

func baseExportConfig() exportConfig {
	return exportConfig{
		Boards:         []string{"TQBR", "TQOB", "FUT"},
		AllTradesCodes: map[string]struct{}{"SBER": {}, "SU26238RMFS9": {}, "MXZ6": {}},
	}
}

func TestBuildExportPlanSelectsFeeds(t *testing.T) {
	cfg := baseExportConfig()
	plan := buildExportPlan(testCatalog(), cfg)

	if got, want := len(plan.AllTrades), 3; got != want {
		t.Fatalf("alltrades = %d, want %d: %+v", got, want, plan.AllTrades)
	}
	if plan.AllTrades[0].SecCode != "SBER" || plan.AllTrades[1].SecCode != "SU26238RMFS9" {
		t.Fatalf("порядок alltrades: %+v", plan.AllTrades)
	}
	if len(plan.Quotations) != 0 {
		t.Fatalf("без EXPORT_SEC_CODES котировок быть не должно: %+v", plan.Quotations)
	}
	if len(plan.History) != 0 {
		t.Fatalf("без EXPORT_SEC_CODES истории быть не должно: %+v", plan.History)
	}

	// Только активные, с secid != 0 и коротким кодом; доска не важна.
	if got, want := len(plan.Securities), 5; got != want {
		t.Fatalf("securities = %d, want %d: %+v", got, want, plan.Securities)
	}
	for _, sec := range plan.Securities {
		if sec.SecID == 0 || !sec.Active || len(sec.SecCode) > 16 {
			t.Fatalf("в снимок попала невалидная бумага: %+v", sec)
		}
	}
}

func TestBuildExportPlanAllTradesAndPositions(t *testing.T) {
	t.Setenv("EXPORT_SEC_BOARDS", "TQBR")
	t.Setenv("EXPORT_ALL_TRADES", "positions,ALL")
	cfg := exportConfigFromEnv()
	plan := buildExportPlan(testCatalog(), cfg)

	if !cfg.Positions {
		t.Fatal("positions не распознан")
	}
	// ALL берёт все бумаги на доске ещё до проверки active: SBER, GAZP, OLD, NOSECID, TOOLONG.
	if got, want := len(plan.AllTrades), 5; got != want {
		t.Fatalf("alltrades = %d, want %d: %+v", got, want, plan.AllTrades)
	}
}

func TestBuildExportPlanQuotationsAndHistory(t *testing.T) {
	cfg := baseExportConfig()
	cfg.SecCodes = []string{"ALL"}
	cfg.PeriodSeconds = []string{"60", "86400"}
	cfg.CandleCount = 10

	catalog := testCatalog()
	plan := buildExportPlan(catalog, cfg)

	// MCT вне досок, OLD/NOSECID/TOOLONG отсеяны как неактивные/битые.
	if got, want := len(plan.Quotations), 4; got != want {
		t.Fatalf("quotations = %d, want %d: %+v", got, want, plan.Quotations)
	}
	// 4 бумаги × 2 периода.
	if got, want := len(plan.History), 8; got != want {
		t.Fatalf("history = %d, want %d: %+v", got, want, plan.History)
	}
	if plan.History[0].Kind.Period != 60 {
		t.Fatalf("фильтр EXPORT_PERIOD_SECONDS не применён: %+v", plan.History[:2])
	}
}

func TestBuildExportPlanCandleCountZeroDisablesHistory(t *testing.T) {
	cfg := baseExportConfig()
	cfg.SecCodes = []string{"ALL"}
	cfg.CandleCount = 0
	plan := buildExportPlan(testCatalog(), cfg)
	if len(plan.Quotations) != 4 {
		t.Fatalf("quotations = %d, want 4", len(plan.Quotations))
	}
	if len(plan.History) != 0 {
		t.Fatalf("при EXPORT_CANDLE_COUNT=0 истории быть не должно: %+v", plan.History)
	}
}

func TestBuildExportPlanSecInfo(t *testing.T) {
	cfg := baseExportConfig()
	cfg.SecInfoAll = true
	plan := buildExportPlan(testCatalog(), cfg)
	if got, want := len(plan.SecInfo), 1; got != want || plan.SecInfo[0].SecCode != "SU26238RMFS9" {
		t.Fatalf("sec_info = %+v, want только облигацию", plan.SecInfo)
	}

	cfg.SecInfoAll = false
	cfg.SecInfoNames = []string{"26238", "нет такой"}
	plan = buildExportPlan(testCatalog(), cfg)
	if got, want := len(plan.SecInfo), 1; got != want {
		t.Fatalf("sec_info по суффиксу = %+v", plan.SecInfo)
	}
}

func TestBuildExportPlanMatchingRules(t *testing.T) {
	catalog := Catalog{Securities: []Security{
		{SecID: 1, Active: true, SecCode: "SBERP", ShortName: "Сбербанк-п", Board: "TQBR"},
		{SecID: 2, Active: true, SecCode: "GAZP", ShortName: "Газпром", Board: "TQBR"},
	}}
	cfg := exportConfig{
		Boards:         []string{"TQBR"},
		AllTradesCodes: map[string]struct{}{},
		SecCodes:       []string{"SBER", "Газпром"},
	}
	plan := buildExportPlan(catalog, cfg)
	if len(plan.Quotations) != 2 {
		t.Fatalf("Contains и ShortName должны совпасть: %+v", plan.Quotations)
	}
}

func TestExportConfigFromEnv(t *testing.T) {
	t.Setenv("EXPORT_SEC_BOARDS", "TQBR,FUT")
	t.Setenv("EXPORT_ALL_TRADES", "SBER,positions,MXZ6")
	t.Setenv("EXPORT_SEC_CODES", "ALL")
	t.Setenv("EXPORT_SEC_INFO_NAMES", "ALL")
	t.Setenv("EXPORT_PERIOD_SECONDS", "60,300")
	t.Setenv("EXPORT_CANDLE_COUNT", "100")

	cfg := exportConfigFromEnv()
	if len(cfg.Boards) != 2 || cfg.Boards[0] != "TQBR" {
		t.Fatalf("boards = %v", cfg.Boards)
	}
	if !cfg.AllTradesAll && len(cfg.AllTradesCodes) != 2 {
		t.Fatalf("alltrades = %+v", cfg)
	}
	if !cfg.Positions || !cfg.SecInfoAll || cfg.CandleCount != 100 {
		t.Fatalf("config = %+v", cfg)
	}
	if len(cfg.SecCodes) != 1 || cfg.SecCodes[0] != "ALL" {
		t.Fatalf("sec codes = %v", cfg.SecCodes)
	}
	if len(cfg.PeriodSeconds) != 2 {
		t.Fatalf("periods = %v", cfg.PeriodSeconds)
	}
}

func TestExportConfigCandleCountBounds(t *testing.T) {
	t.Setenv("EXPORT_CANDLE_COUNT", "-2")
	if got := exportConfigFromEnv().CandleCount; got != ExportCandleCount {
		t.Fatalf("count = %d, want default %d", got, ExportCandleCount)
	}
	t.Setenv("EXPORT_CANDLE_COUNT", "-1")
	if got := exportConfigFromEnv().CandleCount; got != -1 {
		t.Fatalf("count = %d, want -1", got)
	}
}
