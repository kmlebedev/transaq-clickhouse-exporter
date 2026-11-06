package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeSession struct {
	run        func(context.Context, func(Event) error) error
	closeCount int
}

func (s *fakeSession) Run(ctx context.Context, handler func(Event) error) error {
	if s.run != nil {
		return s.run(ctx, handler)
	}
	<-ctx.Done()
	return nil
}

func (s *fakeSession) Close() error {
	s.closeCount++
	return nil
}

type fakeProvider struct {
	mu      sync.Mutex
	opens   int
	factory func(attempt int) (Session, error)
}

func (p *fakeProvider) Name() string { return "fake" }

func (p *fakeProvider) Open(context.Context) (Session, error) {
	p.mu.Lock()
	p.opens++
	attempt := p.opens
	p.mu.Unlock()
	return p.factory(attempt)
}

func TestRunExporterReopensAfterFailure(t *testing.T) {
	provider := &fakeProvider{factory: func(attempt int) (Session, error) {
		if attempt == 1 {
			return &fakeSession{run: func(context.Context, func(Event) error) error {
				return errors.New("session failed")
			}}, nil
		}
		return &fakeSession{run: func(context.Context, func(Event) error) error {
			return nil
		}}, nil
	}}
	err := runExporter(context.Background(), provider, exportConfig{}, reconnectConfig{
		RetryMin:           time.Millisecond,
		RetryMax:           2 * time.Millisecond,
		SessionStableAfter: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if provider.opens != 2 {
		t.Fatalf("opens = %d, want 2", provider.opens)
	}
}

func TestRunExporterRetriesOpenErrors(t *testing.T) {
	provider := &fakeProvider{factory: func(attempt int) (Session, error) {
		if attempt < 3 {
			return nil, errors.New("dial failed")
		}
		return &fakeSession{run: func(context.Context, func(Event) error) error {
			return nil
		}}, nil
	}}
	err := runExporter(context.Background(), provider, exportConfig{}, reconnectConfig{
		RetryMin:           time.Millisecond,
		RetryMax:           2 * time.Millisecond,
		SessionStableAfter: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if provider.opens != 3 {
		t.Fatalf("opens = %d, want 3", provider.opens)
	}
}

func TestRunExporterStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	provider := &fakeProvider{factory: func(int) (Session, error) {
		return &fakeSession{run: func(ctx context.Context, _ func(Event) error) error {
			<-ctx.Done()
			return nil
		}}, nil
	}}
	done := make(chan error, 1)
	go func() {
		done <- runExporter(ctx, provider, exportConfig{}, defaultReconnectConfig())
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("runExporter не остановился по отмене контекста")
	}
}

type catalogSession struct {
	catalog      Catalog
	subscribeErr error
	subscribed   chan Subscription
	subscribedN  int
	mu           sync.Mutex
}

func (s *catalogSession) Catalog() Catalog { return s.catalog }

func (s *catalogSession) Subscribe(_ context.Context, sub Subscription) error {
	s.mu.Lock()
	s.subscribedN++
	s.mu.Unlock()
	select {
	case s.subscribed <- sub:
	default:
	}
	return s.subscribeErr
}

func (s *catalogSession) Run(ctx context.Context, _ func(Event) error) error {
	<-ctx.Done()
	return nil
}

func (s *catalogSession) Close() error { return nil }

func oneSecurityCatalog() Catalog {
	return Catalog{Securities: []Security{{
		SecID: 1, Active: true, SecCode: "SBER", ShortName: "Сбербанк", Board: "TQBR", SecType: "SHARE",
	}}}
}

func TestRunExporterSessionSubscribesPlan(t *testing.T) {
	previousConnect := connect
	recorder := &recordingConn{}
	connect = recorder
	defer func() { connect = previousConnect }()

	t.Setenv("EXPORT_SEC_BOARDS", "TQBR")
	t.Setenv("EXPORT_ALL_TRADES", "SBER")
	t.Setenv("EXPORT_SEC_CODES", "")
	t.Setenv("EXPORT_SEC_INFO_NAMES", "")

	session := &catalogSession{catalog: oneSecurityCatalog(), subscribed: make(chan Subscription, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runExporterSession(ctx, session, exportConfigFromEnv()) }()

	var sub Subscription
	select {
	case sub = <-session.subscribed:
	case <-time.After(time.Second):
		cancel()
		<-done
		t.Fatal("подписка не отправлена")
	}
	if len(sub.AllTrades) != 1 || sub.AllTrades[0].SecCode != "SBER" {
		t.Fatalf("subscription = %+v", sub)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if session.subscribedN != 1 {
		t.Fatalf("subscribe calls = %d, want 1", session.subscribedN)
	}
	if recorder.query != ChSecuritiesInsertQuery {
		t.Fatalf("securities insert query = %q", recorder.query)
	}
}

func TestRunExporterSessionSubscriptionFailure(t *testing.T) {
	previousConnect := connect
	connect = &recordingConn{}
	defer func() { connect = previousConnect }()

	t.Setenv("EXPORT_SEC_BOARDS", "TQBR")
	t.Setenv("EXPORT_ALL_TRADES", "SBER")
	t.Setenv("EXPORT_SEC_CODES", "")
	t.Setenv("EXPORT_SEC_INFO_NAMES", "")

	session := &catalogSession{
		catalog:      oneSecurityCatalog(),
		subscribeErr: errors.New("temporary restore failure"),
		subscribed:   make(chan Subscription, 1),
	}
	err := runExporterSession(context.Background(), session, exportConfigFromEnv())
	if err == nil || !strings.Contains(err.Error(), "temporary restore failure") {
		t.Fatalf("error = %v", err)
	}
}

func TestRunExporterSessionEmptySubscription(t *testing.T) {
	previousConnect := connect
	connect = &recordingConn{}
	defer func() { connect = previousConnect }()

	t.Setenv("EXPORT_SEC_BOARDS", "TQBR")
	t.Setenv("EXPORT_ALL_TRADES", "SBER")
	t.Setenv("EXPORT_SEC_CODES", "")
	t.Setenv("EXPORT_SEC_INFO_NAMES", "")

	catalog := Catalog{Securities: []Security{{
		SecID: 1, Active: true, SecCode: "SBER", Board: "MCT",
	}}}
	session := &catalogSession{catalog: catalog, subscribed: make(chan Subscription, 1)}
	err := runExporterSession(context.Background(), session, exportConfigFromEnv())
	if err == nil || !strings.Contains(err.Error(), "catalog is not ready") {
		t.Fatalf("error = %v", err)
	}
}

func TestCheckSecuritiesInfo(t *testing.T) {
	cases := []struct {
		requested, failed int
		wantErr           bool
	}{
		{requested: 0, failed: 0},
		{requested: 4071, failed: 0},
		{requested: 4071, failed: 1},
		{requested: 4075, failed: 4030, wantErr: true}, // 2026-11-01 05:54 МСК
		{requested: 2, failed: 1},
	}
	for _, tc := range cases {
		err := checkSecuritiesInfo(tc.requested, tc.failed)
		if (err != nil) != tc.wantErr {
			t.Errorf("checkSecuritiesInfo(%d, %d) = %v, wantErr %v", tc.requested, tc.failed, err, tc.wantErr)
		}
	}
}

func TestCheckSubscriptions(t *testing.T) {
	cases := []struct {
		catalog, trades, quotes int
		wantErr                 bool
	}{
		{catalog: 30000, trades: 105, quotes: 105},
		{catalog: 30000, trades: 105, quotes: 0},
		{catalog: 30000, trades: 0, quotes: 3},
		{catalog: 1200, trades: 0, quotes: 0, wantErr: true}, // 2026-11-03 05:54 МСК
	}
	for _, tc := range cases {
		err := checkSubscriptions(tc.catalog, tc.trades, tc.quotes)
		if (err != nil) != tc.wantErr {
			t.Errorf("checkSubscriptions(%d, %d, %d) = %v, wantErr %v", tc.catalog, tc.trades, tc.quotes, err, tc.wantErr)
		}
	}
}
