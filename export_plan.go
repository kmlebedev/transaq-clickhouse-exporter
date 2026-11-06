package main

import (
	"os"
	"slices"
	"strconv"
	"strings"
)

// exportConfig is the parsed EXPORT_* filter set. It is transport-neutral: the
// same plan is executed by the native and the legacy gRPC provider.
type exportConfig struct {
	Boards         []string
	AllTradesAll   bool
	AllTradesCodes map[string]struct{}
	Positions      bool
	SecCodes       []string
	SecInfoAll     bool
	SecInfoNames   []string
	PeriodSeconds  []string
	CandleCount    int
}

// exportConfigFromEnv reads the filters exactly as the pre-provider code did.
func exportConfigFromEnv() exportConfig {
	universe := exportUniverseFromEnv()
	cfg := exportConfig{
		Boards:         universe.boards,
		AllTradesAll:   universe.all,
		AllTradesCodes: universe.codes,
		Positions:      universe.positions,
		CandleCount:    ExportCandleCount,
	}
	if codes := os.Getenv("EXPORT_SEC_CODES"); codes != "" {
		cfg.SecCodes = strings.Split(codes, ",")
	}
	if names := os.Getenv("EXPORT_SEC_INFO_NAMES"); names != "" {
		cfg.SecInfoNames = strings.Split(names, ",")
	}
	cfg.SecInfoAll = slices.Contains(cfg.SecInfoNames, "ALL")
	if periods := os.Getenv("EXPORT_PERIOD_SECONDS"); periods != "" {
		cfg.PeriodSeconds = strings.Split(periods, ",")
	}
	if count, err := strconv.Atoi(os.Getenv("EXPORT_CANDLE_COUNT")); err == nil && count > -2 {
		cfg.CandleCount = count
	}
	return cfg
}

// historyRequest is one gethistorydata query selected by the plan.
type historyRequest struct {
	Security Security
	Kind     CandleKind
}

// exportPlan is the result of applying EXPORT_* to the session catalog.
type exportPlan struct {
	Securities  []Security
	AllTrades   []Security
	Quotations  []Security
	SecInfo     []Security
	History     []historyRequest
	CandleCount int
}

// Subscription renders the plan as the provider feed selection.
func (p exportPlan) Subscription() Subscription {
	return Subscription{
		AllTrades:  symbolsOf(p.AllTrades),
		Quotations: symbolsOf(p.Quotations),
	}
}

func (p exportPlan) secInfoSymbols() []Symbol {
	return symbolsOf(p.SecInfo)
}

// subscribedSecIDs is the catalog-log "subscribed" set.
func (p exportPlan) subscribedSecIDs() map[int]struct{} {
	ids := make(map[int]struct{}, len(p.AllTrades))
	for _, sec := range p.AllTrades {
		ids[sec.SecID] = struct{}{}
	}
	return ids
}

func symbolsOf(items []Security) []Symbol {
	symbols := make([]Symbol, 0, len(items))
	for _, sec := range items {
		symbols = append(symbols, sec.Symbol())
	}
	return symbols
}

type secInfoKey struct {
	SecID   int
	Market  int
	SecCode string
}

// buildExportPlan reproduces the selection rules of the old updateSecurities:
// boards and codes, the ALL and positions modes, bond sec_info suffixes and the
// candle period filter. Securities for the ClickHouse snapshot are collected
// independently of the feed selection.
func buildExportPlan(catalog Catalog, cfg exportConfig) exportPlan {
	plan := exportPlan{CandleCount: cfg.CandleCount}
	seenTrades := make(map[int]struct{})
	seenSecInfo := make(map[secInfoKey]struct{})

	for _, sec := range catalog.Securities {
		inBoard := slices.Contains(cfg.Boards, sec.Board)
		if inBoard && (cfg.AllTradesAll || hasCode(cfg.AllTradesCodes, sec.SecCode)) {
			if _, seen := seenTrades[sec.SecID]; !seen {
				seenTrades[sec.SecID] = struct{}{}
				plan.AllTrades = append(plan.AllTrades, sec)
			}
		}
		if sec.SecType == "BOND" {
			key := secInfoKey{SecID: sec.SecID, Market: sec.Market, SecCode: sec.SecCode}
			if _, seen := seenSecInfo[key]; !seen && wantSecInfo(sec, cfg) {
				seenSecInfo[key] = struct{}{}
				plan.SecInfo = append(plan.SecInfo, sec)
			}
		}
		if sec.SecID == 0 || !sec.Active || len(sec.SecCode) > 16 {
			continue
		}
		plan.Securities = append(plan.Securities, sec)
		if !inBoard || len(cfg.SecCodes) == 0 {
			continue
		}
		if !matchExportCode(sec, cfg.SecCodes) {
			continue
		}
		plan.Quotations = append(plan.Quotations, sec)
		if cfg.CandleCount == 0 {
			continue
		}
		for _, kind := range catalog.CandleKinds {
			if !periodSelected(kind, cfg.PeriodSeconds) {
				continue
			}
			plan.History = append(plan.History, historyRequest{Security: sec, Kind: kind})
		}
	}
	return plan
}

func hasCode(codes map[string]struct{}, code string) bool {
	_, ok := codes[code]
	return ok
}

func wantSecInfo(sec Security, cfg exportConfig) bool {
	if cfg.SecInfoAll {
		return true
	}
	for _, name := range cfg.SecInfoNames {
		if strings.HasSuffix(sec.ShortName, name) {
			return true
		}
	}
	return false
}

// matchExportCode keeps the historical matching, including Contains and the
// short-name match.
func matchExportCode(sec Security, codes []string) bool {
	for _, code := range codes {
		if code == sec.SecCode || strings.Contains(sec.SecCode, code) ||
			code == sec.ShortName || code == "ALL" {
			return true
		}
	}
	return false
}

func periodSelected(kind CandleKind, periods []string) bool {
	if len(periods) == 0 {
		return true
	}
	seconds := strconv.Itoa(kind.Period)
	return slices.Contains(periods, seconds)
}
