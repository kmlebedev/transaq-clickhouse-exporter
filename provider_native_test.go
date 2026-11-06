package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kmlebedev/txmlconnector/native"
	"github.com/kmlebedev/txmlconnector/native/model"
	"github.com/kmlebedev/txmlconnector/native/records"
	"github.com/kmlebedev/txmlconnector/native/wire"
)

func testNativeCatalog() *records.Catalog {
	startup := &model.Startup{
		Boards:      []model.Board{{Num: 1, Code: "TQBR", Byte2: 2}},
		SecTypes:    []model.SecType{{ID: 0}},
		CandleKinds: []model.CandleKind{{ID: 1, Period: 60, Name: "1 мин"}},
	}
	catalog := records.NewCatalog(startup)
	catalog.AddSecurity(records.SecNew{
		SecID:     1,
		Market:    1,
		Kind:      0,
		Class:     'E',
		SecCode:   "SBER",
		ShortName: "Сбербанк",
		Flags:     3,
		Pits: []records.Pit{{
			Board: 1, LotSize: 10, LotDivider: 1, MinStep: 100,
			Decimals: 2, Default: true, PointCost: 1,
		}},
	})
	return catalog
}

type fakeNativeClient struct {
	catalog    *records.Catalog
	subscripts []native.Subscription
	history    model.Candles
	positions  []model.SecPosition
	specReply  []byte
	runErr     error
	runEvents  []model.Event
	stats      native.Stats
	closed     bool
}

func (f *fakeNativeClient) Catalog() *records.Catalog { return f.catalog }

func (f *fakeNativeClient) Subscribe(_ context.Context, sub native.Subscription) error {
	f.subscripts = append(f.subscripts, sub)
	return nil
}

func (f *fakeNativeClient) History(context.Context, native.HistoryRequest) (model.Candles, error) {
	return f.history, nil
}

func (f *fakeNativeClient) Run(ctx context.Context, _ native.RunOptions, h func([]model.Event) error) error {
	if len(f.runEvents) > 0 {
		if err := h(f.runEvents); err != nil {
			return err
		}
	}
	if f.runErr != nil {
		return f.runErr
	}
	<-ctx.Done()
	return ctx.Err()
}

func (f *fakeNativeClient) SecPositions() []model.SecPosition { return f.positions }

func (f *fakeNativeClient) SecSpec(_ context.Context, _ []uint16) ([]byte, error) {
	return f.specReply, nil
}

func (f *fakeNativeClient) Stats() native.Stats { return f.stats }

func (f *fakeNativeClient) Logout(context.Context) error { return nil }

func (f *fakeNativeClient) Close() error {
	f.closed = true
	return nil
}

func newTestNativeSession(t *testing.T) (*nativeSession, *fakeNativeClient) {
	t.Helper()
	fake := &fakeNativeClient{catalog: testNativeCatalog()}
	session, err := newNativeSession(fake)
	if err != nil {
		t.Fatal(err)
	}
	return session, fake
}

func TestNativeSessionCatalog(t *testing.T) {
	session, _ := newTestNativeSession(t)
	catalog := session.Catalog()
	if len(catalog.Securities) != 1 {
		t.Fatalf("securities = %+v", catalog.Securities)
	}
	sec := catalog.Securities[0]
	if sec.SecID != 1 || sec.SecCode != "SBER" || sec.Board != "TQBR" || sec.SecType != "SHARE" || !sec.Active {
		t.Fatalf("security = %+v", sec)
	}
	if len(catalog.CandleKinds) != 1 || catalog.CandleKinds[0].ID != 1 || catalog.CandleKinds[0].Period != 60 {
		t.Fatalf("candle kinds = %+v", catalog.CandleKinds)
	}
}

func TestNativeSessionSubscribe(t *testing.T) {
	session, fake := newTestNativeSession(t)
	err := session.Subscribe(context.Background(), Subscription{
		AllTrades:  []Symbol{{Board: "TQBR", SecCode: "SBER"}},
		Quotations: []Symbol{{Board: "TQBR", SecCode: "SBER"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(fake.subscripts) != 2 {
		t.Fatalf("subscriptions = %+v", fake.subscripts)
	}
	if fake.subscripts[0].AllTrades[0].SecCode != "SBER" || len(fake.subscripts[1].Quotations) != 1 {
		t.Fatalf("subscriptions = %+v", fake.subscripts)
	}
}

func TestNativeSessionNormalizeEvents(t *testing.T) {
	session, _ := newTestNativeSession(t)

	trade, ok := session.normalize(&model.Trade{
		SecID: 1, Board: "TQBR", SecCode: "SBER", TradeNo: 7,
		Time:     time.Unix(100, 0).UTC(),
		Price:    model.Decimal{Units: 30512, Scale: 2},
		Quantity: 3, BuySell: 'B', Period: 'N', OpenInterest: 5,
	})
	if !ok {
		t.Fatal("trade не нормализован")
	}
	tradeEvent := trade.(TradeEvent)
	if tradeEvent.Price != 305.12 || tradeEvent.BuySell != "B" || tradeEvent.Period != "N" || tradeEvent.Quantity != 3 {
		t.Fatalf("trade = %+v", tradeEvent)
	}

	quotes, ok := session.normalize(&model.Quotes{
		SecID: 1, Board: "TQBR", Symbol: model.Symbol{SecCode: "SBER"},
		Levels: []model.Quote{{Price: model.Decimal{Units: 100, Scale: 2}, Buy: 4, Sell: -1}},
	})
	if !ok {
		t.Fatal("quotes не нормализованы")
	}
	quotesEvent := quotes.(QuotesEvent)
	if quotesEvent.Levels[0].Price != 1 || quotesEvent.Levels[0].Sell != -1 {
		t.Fatalf("quotes = %+v", quotesEvent)
	}

	time10 := "10:00:00"
	open, last := model.Decimal{Units: 100, Scale: 2}, model.Decimal{Units: 101, Scale: 2}
	quantity := int64(2)
	quotation, ok := session.normalize(&model.Quotation{
		SecID: 1, Board: "TQBR", Symbol: model.Symbol{SecCode: "SBER"},
		Fields: model.QuotationFields{Time: &time10, Open: &open, Last: &last, Quantity: &quantity},
	})
	if !ok {
		t.Fatal("quotation не нормализована")
	}
	quotationEvent := quotation.(QuotationEvent)
	if quotationEvent.Time != "10:00:00" || quotationEvent.Last != 1.01 || quotationEvent.Quantity != 2 {
		t.Fatalf("quotation = %+v", quotationEvent)
	}

	status, ok := session.normalize(model.ServerStatus{Connected: true, Recover: true})
	if !ok || status.(StatusEvent).Recover != true {
		t.Fatalf("status = %+v", status)
	}
	unknown, ok := session.normalize(model.UnknownRecord{Tag: 0x99, Offset: 3})
	if !ok || unknown.(UnknownEvent).Tag != 0x99 {
		t.Fatalf("unknown = %+v", unknown)
	}
}

func TestNativeSessionSecuritiesInfo(t *testing.T) {
	session, fake := newTestNativeSession(t)
	var w wire.Writer
	w.Uint16(1)
	mask := int64(1<<3 | 1<<8 | 1<<9 | 1<<10 | 1<<11 | 1<<36)
	w.Int64(mask)
	w.Int64(100000) // facevalue, hundredths
	w.Int32(20000)  // mat_date, days
	w.Int32(1234)   // coupon_value, hundredths
	w.Int32(182)    // coupon_period
	w.Int32(20090)  // coupon_date, days
	w.String("RU000A0JS3W6", 12)
	fake.specReply = w.Bytes()

	infos, failed, err := session.SecuritiesInfo(context.Background(), []Symbol{{Board: "TQBR", SecCode: "SBER"}})
	if err != nil {
		t.Fatal(err)
	}
	if failed != 0 {
		t.Fatalf("failed = %d, want 0", failed)
	}
	if len(infos) != 1 {
		t.Fatalf("infos = %+v", infos)
	}
	info := infos[0]
	if info.FaceValue != 1000 || info.CouponPeriod != 182 || info.ISIN != "RU000A0JS3W6" {
		t.Fatalf("info = %+v", info)
	}
	if info.MatDate == nil || info.CouponDate == nil {
		t.Fatalf("даты не разобраны: %+v", info)
	}
}

func TestNativeSessionPositions(t *testing.T) {
	session, fake := newTestNativeSession(t)
	fake.positions = []model.SecPosition{{SecID: 1, SecCode: "SBER"}}
	positions := session.Positions()
	if len(positions) != 1 || positions[0] != (Symbol{Board: "TQBR", SecCode: "SBER"}) {
		t.Fatalf("positions = %+v", positions)
	}
}

func TestNativeSessionRunDeliversNormalizedEvents(t *testing.T) {
	session, fake := newTestNativeSession(t)
	fake.runEvents = []model.Event{&model.Trade{
		SecID: 1, Board: "TQBR", SecCode: "SBER",
		Price: model.Decimal{Units: 100, Scale: 2}, Quantity: 1,
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var got []Event
	done := make(chan error, 1)
	go func() {
		done <- session.Run(ctx, func(event Event) error {
			got = append(got, event)
			cancel()
			return nil
		})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run не завершился")
	}
	if len(got) != 1 {
		t.Fatalf("events = %+v", got)
	}
	if _, ok := got[0].(TradeEvent); !ok {
		t.Fatalf("event = %T", got[0])
	}
}

func TestNativeProviderOpenUsesDialHook(t *testing.T) {
	setTransaqEnv(t)
	fake := &fakeNativeClient{catalog: testNativeCatalog()}
	previous := dialNativeClient
	dialNativeClient = func(context.Context, native.ClientConfig) (nativeSessionClient, error) {
		return fake, nil
	}
	t.Cleanup(func() { dialNativeClient = previous })

	provider, err := newNativeProvider()
	if err != nil {
		t.Fatal(err)
	}
	session, err := provider.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := session.(*nativeSession); !ok {
		t.Fatalf("session = %T", session)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if !fake.closed {
		t.Fatal("Close не закрыл native-клиент")
	}
}

func TestNativeProviderDialError(t *testing.T) {
	setTransaqEnv(t)
	previous := dialNativeClient
	dialNativeClient = func(context.Context, native.ClientConfig) (nativeSessionClient, error) {
		return nil, errors.New("dial failed")
	}
	t.Cleanup(func() { dialNativeClient = previous })

	provider, err := newNativeProvider()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Open(context.Background()); err == nil {
		t.Fatal("Open вернул сессию при ошибке Dial")
	}
}

func TestNativeSessionHistory(t *testing.T) {
	session, fake := newTestNativeSession(t)
	fake.history = model.Candles{Items: []model.Candle{{
		Time:   time.Unix(100, 0).UTC(),
		Open:   model.Decimal{Units: 100, Scale: 2},
		Close:  model.Decimal{Units: 101, Scale: 2},
		High:   model.Decimal{Units: 102, Scale: 2},
		Low:    model.Decimal{Units: 99, Scale: 2},
		Volume: 10,
	}}}
	candles, err := session.History(context.Background(), HistoryRequest{
		Symbol: Symbol{Board: "TQBR", SecCode: "SBER"},
		Period: 1,
		Count:  10,
		Reset:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(candles) != 1 || candles[0].Close != 1.01 || candles[0].Volume != 10 {
		t.Fatalf("candles = %+v", candles)
	}
}
