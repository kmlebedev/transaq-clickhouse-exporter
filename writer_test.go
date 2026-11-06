package main

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestTradeBatcherFlushesOnClose(t *testing.T) {
	previousConnect := connect
	recorder := &recordingConn{}
	connect = recorder
	defer func() { connect = previousConnect }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	batcher := newTradeBatcher(ctx, time.Hour)
	batcher.Write(TradeEvent{
		SecID: 1, SecCode: "SBER", Board: "TQBR", TradeNo: 1,
		Time: time.Unix(100, 0).UTC(), Price: 100, Quantity: 2, BuySell: "B",
	})
	batcher.Write(TradeEvent{
		SecID: 1, SecCode: "SBER", Board: "TQBR", TradeNo: 2,
		Time: time.Unix(101, 0).UTC(), Price: 101, Quantity: 3, BuySell: "S",
	})
	batcher.Close()

	if recorder.query != ChTradesInsertQuery {
		t.Fatalf("query = %q, want %q", recorder.query, ChTradesInsertQuery)
	}
	if recorder.batch == nil || len(recorder.batch.rows) != 2 {
		t.Fatalf("rows = %+v, want 2", recorder.batch)
	}
	if !recorder.batch.sent {
		t.Fatal("батч сделок не отправлен")
	}
}

func TestInsertCandleRows(t *testing.T) {
	previousConnect := connect
	recorder := &recordingConn{}
	connect = recorder
	defer func() { connect = previousConnect }()

	err := insertCandleRows(context.Background(), []candleRow{{
		Time: time.Unix(100, 0).UTC(), SecCode: "SBER", Period: 1,
		Open: 1, Close: 2, High: 3, Low: 0.5, Volume: 10,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if recorder.query != ChCandlesInsertQuery || len(recorder.batch.rows) != 1 || len(recorder.batch.rows[0]) != 8 {
		t.Fatalf("candles batch = %+v", recorder.batch)
	}
	if !recorder.batch.sent {
		t.Fatal("батч свечей не отправлен")
	}
}

type fakeHistorySource struct {
	mu       sync.Mutex
	requests []HistoryRequest
	candles  []Candle
	err      error
}

func (f *fakeHistorySource) History(_ context.Context, req HistoryRequest) ([]Candle, error) {
	f.mu.Lock()
	f.requests = append(f.requests, req)
	f.mu.Unlock()
	return f.candles, f.err
}

func TestRunHistoryRequestsWritesCandles(t *testing.T) {
	source := &fakeHistorySource{candles: []Candle{{
		Time: time.Unix(100, 0).UTC(), Open: 1, Close: 2, High: 3, Low: 0.5, Volume: 10,
	}}}
	plan := exportPlan{
		CandleCount: 10,
		History: []historyRequest{{
			Security: Security{SecID: 1, SecCode: "SBER", Board: "TQBR"},
			Kind:     CandleKind{ID: 2, Period: 300},
		}},
	}
	var mu sync.Mutex
	var got []candleRow
	var gotCode string
	var gotPeriod int
	runHistoryRequests(context.Background(), source, plan, func(secCode string, period int, items []Candle) {
		mu.Lock()
		defer mu.Unlock()
		gotCode, gotPeriod = secCode, period
		got = candleRows(secCode, period, items)
	})
	if len(source.requests) != 1 {
		t.Fatalf("requests = %+v", source.requests)
	}
	request := source.requests[0]
	if request.Symbol != (Symbol{Board: "TQBR", SecCode: "SBER"}) || request.Period != 2 || request.Count != 10 || !request.Reset {
		t.Fatalf("request = %+v", request)
	}
	if gotCode != "SBER" || gotPeriod != 2 || len(got) != 1 || got[0].Period != 2 || got[0].Volume != 10 {
		t.Fatalf("candles = %+v, code = %q period = %d", got, gotCode, gotPeriod)
	}
}

func TestRunHistoryRequestsAllCandlesAndClamp(t *testing.T) {
	source := &fakeHistorySource{}
	plan := exportPlan{
		CandleCount: -1,
		History: []historyRequest{{
			Security: Security{SecID: 1, SecCode: "SBER", Board: "TQBR"},
			Kind:     CandleKind{ID: 1},
		}},
	}
	runHistoryRequests(context.Background(), source, plan, func(string, int, []Candle) {})
	if source.requests[0].Count != 0 || source.requests[0].Reset {
		t.Fatalf("all-history request = %+v", source.requests[0])
	}

	source = &fakeHistorySource{}
	plan.CandleCount = 70000
	runHistoryRequests(context.Background(), source, plan, func(string, int, []Candle) {})
	if source.requests[0].Count != 65535 {
		t.Fatalf("count = %d, want 65535", source.requests[0].Count)
	}
}

func TestQuoteAndCandleRowsClampToSchema(t *testing.T) {
	quotes := quoteRowsFromEvent(QuotesEvent{
		SecID: 1, Board: "TQBR", SecCode: "SBER",
		Levels: []QuoteLevel{{Price: 1.5, Source: "MM", Yield: 300, Buy: 100000, Sell: -100000}},
	})
	if len(quotes) != 1 || quotes[0].Yield != 127 || quotes[0].Buy != 32767 || quotes[0].Sell != -32768 {
		t.Fatalf("quotes = %+v", quotes)
	}

	rows := candleRows("SBER", 1, []Candle{{Time: time.Unix(100, 0), Volume: -5}})
	if len(rows) != 1 || rows[0].Volume != 0 {
		t.Fatalf("candles = %+v", rows)
	}
}

func TestEventDispatcherHandleTrade(t *testing.T) {
	previousConnect := connect
	recorder := &recordingConn{}
	connect = recorder
	defer func() { connect = previousConnect }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dispatcher := newEventDispatcher(ctx, exportConfig{}, nil, nil)
	if err := dispatcher.Handle(TradeEvent{
		SecID: 1, SecCode: "SBER", Board: "TQBR", TradeNo: 1,
		Time: time.Unix(100, 0).UTC(), Price: 100, Quantity: 2,
	}); err != nil {
		t.Fatal(err)
	}
	dispatcher.Handle(StatusEvent{Connected: true})
	dispatcher.Handle(UnknownEvent{Tag: 0x99, Offset: 1})
	dispatcher.Close()

	if recorder.query != ChTradesInsertQuery || recorder.batch == nil || len(recorder.batch.rows) != 1 {
		t.Fatalf("trades batch = %+v", recorder.batch)
	}
}

type fakeSubscriber struct {
	ch chan Subscription
}

func (f *fakeSubscriber) Subscribe(_ context.Context, sub Subscription) error {
	f.ch <- sub
	return nil
}

func TestPositionSubscriberAddDeduplicates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub := &fakeSubscriber{ch: make(chan Subscription, 4)}
	positions := newPositionSubscriber(ctx, sub, nil)
	defer positions.Close()

	symbol := Symbol{Board: "TQBR", SecCode: "SBER"}
	positions.Add(symbol)
	positions.Add(symbol)
	select {
	case got := <-sub.ch:
		if len(got.AllTrades) != 1 || got.AllTrades[0] != symbol {
			t.Fatalf("subscription = %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("подписка позиции не отправлена")
	}
	select {
	case got := <-sub.ch:
		t.Fatalf("повторная подписка отправлена: %+v", got)
	case <-time.After(50 * time.Millisecond):
	}
}
