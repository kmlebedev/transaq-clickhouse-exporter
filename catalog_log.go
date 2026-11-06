package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

// catalogLogHeartbeat — итог раз в час, даже если каталог не менялся: по нему
// видно, что сессия жива и подписка не пропала.
const catalogLogHeartbeat = time.Hour

// catalogLogEvery — как часто сверять каталог с подпиской для лога роста
// каталога. Переменная, чтобы тесты не ждали минуту.
var catalogLogEvery = time.Minute

// exportUniverse — доски и коды подписки из EXPORT_SEC_BOARDS/EXPORT_ALL_TRADES.
type exportUniverse struct {
	boards    []string
	codes     map[string]struct{}
	all       bool
	positions bool
}

// exportUniverseFromEnv читает конфигурацию так же, как buildExportPlan:
// доски по умолчанию — TQBR, TQCB, FUT; "positions" — режим, а не код бумаги.
func exportUniverseFromEnv() exportUniverse {
	universe := exportUniverse{boards: []string{"TQBR", "TQCB", "FUT"}, codes: make(map[string]struct{})}
	if boards := os.Getenv("EXPORT_SEC_BOARDS"); boards != "" {
		universe.boards = strings.Split(boards, ",")
	}
	if codes := os.Getenv("EXPORT_ALL_TRADES"); codes != "" {
		for _, code := range strings.Split(codes, ",") {
			switch code {
			case "positions":
				universe.positions = true
			case "ALL":
				universe.all = true
			default:
				universe.codes[code] = struct{}{}
			}
		}
	}
	return universe
}

type boardCount struct {
	catalog, universe, subscribed int
}

// catalogSummary — каталог по доскам вселенной: всего бумаг на доске / из них
// во вселенной / из них в подписке. Порядок досок — как в конфигурации.
type catalogSummary struct {
	total  int
	boards []string
	counts map[string]*boardCount
}

func summarizeCatalog(items []Security, universe exportUniverse, subscribed map[int]struct{}) catalogSummary {
	summary := catalogSummary{total: len(items), boards: universe.boards, counts: make(map[string]*boardCount)}
	for _, board := range universe.boards {
		summary.counts[board] = &boardCount{}
	}
	for _, item := range items {
		count, ok := summary.counts[item.Board]
		if !ok {
			continue
		}
		count.catalog++
		if _, inUniverse := universe.codes[item.SecCode]; !inUniverse && !universe.all {
			continue
		}
		count.universe++
		if _, ok = subscribed[item.SecID]; ok {
			count.subscribed++
		}
	}
	return summary
}

func (s catalogSummary) String() string {
	parts := make([]string, 0, len(s.boards))
	for _, board := range s.boards {
		count := s.counts[board]
		parts = append(parts, fmt.Sprintf("%s %d/%d/%d", board, count.catalog, count.universe, count.subscribed))
	}
	return fmt.Sprintf("catalog %d: %s", s.total, strings.Join(parts, ", "))
}

// catalogLogger пишет сводку каталога, когда она изменилась, и раз в час.
// Нужен для проверки гипотезы H1 ofz-market/1.8: догружает ли брокер секцию
// каталога после того, как подписка уже отправлена.
type catalogLogger struct {
	last   string
	lastAt time.Time
}

func (l *catalogLogger) log(items []Security, subscribed map[int]struct{}, now time.Time) {
	summary := summarizeCatalog(items, exportUniverseFromEnv(), subscribed).String()
	if summary == l.last && now.Sub(l.lastAt) < catalogLogHeartbeat {
		return
	}
	l.last, l.lastAt = summary, now
	log.Infof("TRANSAQ catalog (board catalog/universe/subscribed) %s", summary)
}
