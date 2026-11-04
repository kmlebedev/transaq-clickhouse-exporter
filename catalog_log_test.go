package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"

	tcClient "github.com/kmlebedev/txmlconnector/client"
	"github.com/kmlebedev/txmlconnector/client/commands"
)

func TestSummarizeCatalog(t *testing.T) {
	items := []commands.Security{
		{SecId: 1, SecCode: "SBER", Board: "TQBR"},
		{SecId: 2, SecCode: "ABRD", Board: "TQBR"},
		{SecId: 3, SecCode: "SU26238RMFS9", Board: "TQOB"},
		{SecId: 4, SecCode: "MXZ6", Board: "FUT"},
		{SecId: 5, SecCode: "LQDT", Board: "MCT"},
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
	if len(universe.codes) != 2 {
		t.Fatalf("codes = %v", universe.codes)
	}
}

// Гипотеза H1 ofz-market/1.8: после ночного рестарта брокер догружает секцию
// каталога позже подписки (2026-11-04: подписка ушла на 7 фьючерсов, акций и
// ОФЗ в каталоге не было). Лог обязан показать рост каталога после подписки.
func TestProcessTransaqLogsCatalogGrowth(t *testing.T) {
	t.Setenv("EXPORT_SEC_BOARDS", "TQBR,FUT")
	t.Setenv("EXPORT_ALL_TRADES", "SBER,MXZ6")
	saved := allTrades.Items
	t.Cleanup(func() { allTrades.Items = saved })
	hook := logtest.NewGlobal()
	defer hook.Reset()

	catalog := newSecuritiesCatalog()
	client := newTestTCClient(newFakeConnectServiceClient())
	processCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	restored := make(chan struct{}, 1)
	go func() {
		done <- processTransaq(processCtx, client, transaqSessionConfig{
			catalog:         catalog,
			catalogLogEvery: 5 * time.Millisecond,
			restore: func(*tcClient.TCClient) error {
				allTrades.Items = []int{4}
				restored <- struct{}{}
				return nil
			},
		})
	}()
	// Цикл сессии читает allTrades: он обязан остановиться до того, как
	// cleanup вернёт глобальную подписку, иначе гонка.
	stopped := false
	stop := func() error {
		if stopped {
			return nil
		}
		stopped = true
		cancel()
		return <-done
	}
	t.Cleanup(func() { _ = stop() })

	// Сессия сбрасывает каталог при старте, поэтому порции приходят после
	// подписки — как при догрузке каталога брокером.
	client.ServerStatusChan <- commands.ServerStatus{Connected: "true"}
	<-restored
	catalog.add([]commands.Security{{SecId: 4, SecCode: "MXZ6", Board: "FUT"}})
	waitForLog(t, hook, "catalog 1: TQBR 0/0/0, FUT 1/1/1")

	catalog.add([]commands.Security{{SecId: 1, SecCode: "SBER", Board: "TQBR"}})
	waitForLog(t, hook, "catalog 2: TQBR 1/1/0, FUT 1/1/1")

	before := countLogs(hook, "TRANSAQ catalog")
	time.Sleep(30 * time.Millisecond)
	if after := countLogs(hook, "TRANSAQ catalog"); after != before {
		t.Fatalf("без изменений каталога лог повторился: %d → %d", before, after)
	}

	if err := stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("processTransaq error = %v", err)
	}
}

func waitForLog(t *testing.T, hook *logtest.Hook, fragment string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
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
