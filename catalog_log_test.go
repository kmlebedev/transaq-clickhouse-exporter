package main

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
)

func TestSummarizeCatalog(t *testing.T) {
	items := []Security{
		{SecID: 1, SecCode: "SBER", Board: "TQBR"},
		{SecID: 2, SecCode: "ABRD", Board: "TQBR"},
		{SecID: 3, SecCode: "SU26238RMFS9", Board: "TQOB"},
		{SecID: 4, SecCode: "MXZ6", Board: "FUT"},
		{SecID: 5, SecCode: "LQDT", Board: "MCT"},
	}
	universe := exportUniverse{boards: []string{"TQBR", "TQOB", "FUT"},
		codes: map[string]struct{}{"SBER": {}, "SU26238RMFS9": {}, "MXZ6": {}, "GAZP": {}}}

	got := summarizeCatalog(items, universe, nil).String()
	if want := "catalog 5: TQBR 2/1/0, TQOB 1/1/0, FUT 1/1/0"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}

	subscribed := map[int]struct{}{4: {}}
	got = summarizeCatalog(items, universe, subscribed).String()
	if want := "catalog 5: TQBR 2/1/0, TQOB 1/1/0, FUT 1/1/1"; got != want {
		t.Fatalf("summary = %q, want %q", got, want)
	}

	all := exportUniverse{boards: []string{"TQBR"}, all: true}
	if got, want := summarizeCatalog(items, all, nil).String(), "catalog 5: TQBR 2/2/0"; got != want {
		t.Fatalf("ALL summary = %q, want %q", got, want)
	}
}

func TestExportUniverseFromEnv(t *testing.T) {
	t.Setenv("EXPORT_SEC_BOARDS", "TQBR,FUT")
	t.Setenv("EXPORT_ALL_TRADES", "SBER,positions,MXZ6")
	universe := exportUniverseFromEnv()
	if strings.Join(universe.boards, ",") != "TQBR,FUT" || universe.all {
		t.Fatalf("universe = %+v", universe)
	}
	if _, ok := universe.codes["positions"]; ok {
		t.Fatal("positions — режим, а не код бумаги")
	}
	if !universe.positions {
		t.Fatal("режим positions не распознан")
	}
	if len(universe.codes) != 2 {
		t.Fatalf("codes = %v", universe.codes)
	}
}

// mutableCatalogSource — каталог, который «догружается» после подписки: лог
// обязан показать рост (гипотеза H1 ofz-market/1.8).
type mutableCatalogSource struct {
	mu    sync.Mutex
	items []Security
}

func (s *mutableCatalogSource) Catalog() Catalog {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Catalog{Securities: append([]Security(nil), s.items...)}
}

func (s *mutableCatalogSource) set(items []Security) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = items
}

func TestCatalogLoggerLogsGrowth(t *testing.T) {
	t.Setenv("EXPORT_SEC_BOARDS", "TQBR,FUT")
	t.Setenv("EXPORT_ALL_TRADES", "SBER,MXZ6")
	previousInterval := catalogLogEvery
	catalogLogEvery = 5 * time.Millisecond
	t.Cleanup(func() { catalogLogEvery = previousInterval })

	hook := logtest.NewGlobal()
	defer hook.Reset()

	source := &mutableCatalogSource{}
	source.set([]Security{{SecID: 4, SecCode: "MXZ6", Board: "FUT"}})
	plan := exportPlan{AllTrades: []Security{{SecID: 4, SecCode: "MXZ6", Board: "FUT"}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := startCatalogLogger(ctx, source, true, newSecIDSet(plan.subscribedSecIDs()))
	defer stop()

	waitForLog(t, hook, "catalog 1: TQBR 0/0/0, FUT 1/1/1")
	source.set([]Security{
		{SecID: 4, SecCode: "MXZ6", Board: "FUT"},
		{SecID: 1, SecCode: "SBER", Board: "TQBR"},
	})
	waitForLog(t, hook, "catalog 2: TQBR 1/1/0, FUT 1/1/1")

	before := countLogs(hook, "TRANSAQ catalog")
	time.Sleep(30 * time.Millisecond)
	if after := countLogs(hook, "TRANSAQ catalog"); after != before {
		t.Fatalf("без изменений каталога лог повторился: %d → %d", before, after)
	}
}

func waitForLog(t *testing.T, hook *logtest.Hook, fragment string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if countLogs(hook, fragment) > 0 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	var lines []string
	for _, entry := range hook.AllEntries() {
		lines = append(lines, entry.Message)
	}
	t.Fatalf("нет записи лога с %q; записи: %q", fragment, lines)
}

func countLogs(hook *logtest.Hook, fragment string) int {
	n := 0
	for _, entry := range hook.AllEntries() {
		if entry.Level <= log.InfoLevel && strings.Contains(entry.Message, fragment) {
			n++
		}
	}
	return n
}
