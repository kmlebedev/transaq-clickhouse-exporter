package main

import (
	"context"
	"testing"
	"time"
)

// lateCatalogSession opens with a catalog of one future, like the night
// restart of 2026-11-10 05:55 (FUT 849/7/7, TQBR 0/0/0), and then delivers the
// stock section while running.
type lateCatalogSession struct {
	catalogSession
	late []Security
}

func (s *lateCatalogSession) Run(ctx context.Context, handler func(Event) error) error {
	if err := handler(CatalogAddedEvent{Securities: s.late}); err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}

func TestRunExporterSessionSubscribesLateCatalog(t *testing.T) {
	previousConnect := connect
	connect = &recordingConn{}
	defer func() { connect = previousConnect }()

	t.Setenv("EXPORT_SEC_BOARDS", "TQBR,FUT")
	t.Setenv("EXPORT_ALL_TRADES", "SBER,MXZ6")
	t.Setenv("EXPORT_SEC_CODES", "")
	t.Setenv("EXPORT_SEC_INFO_NAMES", "")

	session := &lateCatalogSession{
		catalogSession: catalogSession{
			catalog: Catalog{Securities: []Security{{
				SecID: 4, Active: true, SecCode: "MXZ6", Board: "FUT", SecType: "FUT",
			}}},
			subscribed: make(chan Subscription, 4),
		},
		late: []Security{
			{SecID: 1, Active: true, SecCode: "SBER", Board: "TQBR", SecType: "SHARE"},
			{SecID: 2, Active: true, SecCode: "ABRD", Board: "TQBR", SecType: "SHARE"}, // не во вселенной
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runExporterSession(ctx, session, exportConfigFromEnv()) }()

	var subs []Subscription
	for len(subs) < 2 {
		select {
		case sub := <-session.subscribed:
			subs = append(subs, sub)
		case <-time.After(time.Second):
			cancel()
			<-done
			t.Fatalf("подписок %d, want 2 (первичная + догруженный каталог): %+v", len(subs), subs)
		}
	}
	if len(subs[0].AllTrades) != 1 || subs[0].AllTrades[0].SecCode != "MXZ6" {
		t.Fatalf("первичная подписка = %+v, want MXZ6", subs[0])
	}
	if len(subs[1].AllTrades) != 1 || subs[1].AllTrades[0].SecCode != "SBER" {
		t.Fatalf("дозаказ = %+v, want только SBER", subs[1])
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// Догруженные бумаги вне вселенной не дают пустого subscribe.
func TestRunExporterSessionLateCatalogOutsideUniverse(t *testing.T) {
	previousConnect := connect
	connect = &recordingConn{}
	defer func() { connect = previousConnect }()

	t.Setenv("EXPORT_SEC_BOARDS", "TQBR,FUT")
	t.Setenv("EXPORT_ALL_TRADES", "MXZ6")
	t.Setenv("EXPORT_SEC_CODES", "")
	t.Setenv("EXPORT_SEC_INFO_NAMES", "")

	session := &lateCatalogSession{
		catalogSession: catalogSession{
			catalog: Catalog{Securities: []Security{{
				SecID: 4, Active: true, SecCode: "MXZ6", Board: "FUT", SecType: "FUT",
			}}},
			subscribed: make(chan Subscription, 4),
		},
		late: []Security{{SecID: 2, Active: true, SecCode: "ABRD", Board: "TQBR", SecType: "SHARE"}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runExporterSession(ctx, session, exportConfigFromEnv()) }()
	<-session.subscribed
	time.Sleep(50 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if session.subscribedN != 1 {
		t.Fatalf("subscribe calls = %d, want 1", session.subscribedN)
	}
}
