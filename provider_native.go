package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/kmlebedev/txmlconnector/native"
	"github.com/kmlebedev/txmlconnector/native/model"
	"github.com/kmlebedev/txmlconnector/native/records"
	"github.com/kmlebedev/txmlconnector/native/xmlcompat"
	log "github.com/sirupsen/logrus"
)

const (
	nativeDialTimeout    = 5 * time.Minute
	nativeRqDelay        = time.Second
	nativeSessionTimeout = 60 * time.Second
	nativeRequestTimeout = 10 * time.Second
	nativePushPeriod     = 30 * time.Second
	nativeSubscribeBatch = 500
	// The RQ_SEC_SPEC reply is one wire frame (65520 B) and the server caps a
	// large list at ~100 records per frame: 50 keeps every chunk in one frame
	// (200 returned only half of the requested specs in the live acceptance).
	nativeSecSpecBatch = 50

	nativeCloseTimeout = 2 * time.Second
)

// nativeSessionClient is the subset of *native.Client the provider uses. It is
// an interface so the session logic is testable without a live terminal.
type nativeSessionClient interface {
	Catalog() *records.Catalog
	Subscribe(ctx context.Context, sub native.Subscription) error
	History(ctx context.Context, req native.HistoryRequest) (model.Candles, error)
	Run(ctx context.Context, opt native.RunOptions, h func([]model.Event) error) error
	SecPositions() []model.SecPosition
	SecSpec(ctx context.Context, ids []uint16) ([]byte, error)
	Stats() native.Stats
	Logout(ctx context.Context) error
	Close() error
}

// nativeClientAdapter adds the RQ_SEC_SPEC access the typed client keeps on its
// session connection.
type nativeClientAdapter struct {
	*native.Client
}

func (a nativeClientAdapter) SecSpec(ctx context.Context, ids []uint16) ([]byte, error) {
	conn := a.Client.Conn()
	if conn == nil {
		return nil, errors.New("transaq: client is not connected")
	}
	return conn.SecSpec(ctx, ids)
}

var dialNativeClient = func(ctx context.Context, cfg native.ClientConfig) (nativeSessionClient, error) {
	client, err := native.Dial(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return nativeClientAdapter{Client: client}, nil
}

type nativeProvider struct {
	cfg native.ClientConfig
}

func (p *nativeProvider) Name() string { return backendNative }

func (p *nativeProvider) Open(ctx context.Context) (Session, error) {
	dialCtx, cancel := context.WithTimeout(ctx, nativeDialTimeout)
	defer cancel()
	client, err := dialNativeClient(dialCtx, p.cfg)
	if err != nil {
		return nil, fmt.Errorf("dial TRANSAQ %s: %w", p.cfg.Address, err)
	}
	session, err := newNativeSession(client)
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	return session, nil
}

func newNativeProvider() (Provider, error) {
	cfg, err := nativeConfigFromEnv()
	if err != nil {
		return nil, err
	}
	return &nativeProvider{cfg: cfg}, nil
}

func nativeConfigFromEnv() (native.ClientConfig, error) {
	login := os.Getenv("TC_LOGIN")
	password := os.Getenv("TC_PASSWORD")
	host := os.Getenv("TC_HOST")
	port := os.Getenv("TC_PORT")
	switch {
	case login == "":
		return native.ClientConfig{}, errors.New("TC_LOGIN is required for the native provider")
	case password == "":
		return native.ClientConfig{}, errors.New("TC_PASSWORD is required for the native provider")
	case host == "":
		return native.ClientConfig{}, errors.New("TC_HOST is required for the native provider")
	case port == "":
		return native.ClientConfig{}, errors.New("TC_PORT is required for the native provider")
	}
	return native.ClientConfig{
		Address:        net.JoinHostPort(host, port),
		Login:          login,
		Password:       password,
		MicexRegisters: true,
		RqDelay:        nativeRqDelay,
		SessionTimeout: nativeSessionTimeout,
		RequestTimeout: nativeRequestTimeout,
		// Stored by the native client; periodic position push timers are not
		// implemented yet, so positions arrive at load and on change only.
		PushULimits:   nativePushPeriod,
		PushPosEquity: nativePushPeriod,
	}, nil
}

func init() { RegisterProvider(backendNative, newNativeProvider) }

type nativeSession struct {
	client nativeSessionClient
	// mu guards catalog, index and secBySymbol: the Run goroutine adds late
	// securities while the catalog logger and sec_info read them.
	mu          sync.RWMutex
	catalog     Catalog
	index       catalogIndex
	secBySymbol map[Symbol]records.SecNew
	closeOnce   sync.Once
	closeErr    error
}

func newNativeSession(client nativeSessionClient) (*nativeSession, error) {
	nativeCatalog := client.Catalog()
	if nativeCatalog == nil || nativeCatalog.Startup() == nil {
		return nil, errors.New("TRANSAQ native catalog is not loaded")
	}
	startup := nativeCatalog.Startup()
	session := &nativeSession{
		client:      client,
		secBySymbol: make(map[Symbol]records.SecNew),
	}
	for _, kind := range startup.CandleKinds {
		session.catalog.CandleKinds = append(session.catalog.CandleKinds, CandleKind{
			ID:     int(kind.ID),
			Period: int(kind.Period),
			Name:   kind.Name,
		})
	}
	for _, sec := range nativeCatalog.List() {
		item := securityFromModel(xmlcompat.Security(startup, sec))
		session.catalog.Securities = append(session.catalog.Securities, item)
		session.secBySymbol[item.Symbol()] = sec
	}
	session.index = newCatalogIndex(session.catalog)
	return session, nil
}

func (s *nativeSession) Catalog() Catalog {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return Catalog{
		Securities:  append([]Security(nil), s.catalog.Securities...),
		CandleKinds: append([]CandleKind(nil), s.catalog.CandleKinds...),
	}
}

func (s *nativeSession) Subscribe(ctx context.Context, sub Subscription) error {
	for _, batch := range chunkSymbols(sub.AllTrades, nativeSubscribeBatch) {
		if err := s.client.Subscribe(ctx, native.Subscription{AllTrades: batch}); err != nil {
			return fmt.Errorf("subscribe alltrades: %w", err)
		}
	}
	for _, batch := range chunkSymbols(sub.Quotations, nativeSubscribeBatch) {
		if err := s.client.Subscribe(ctx, native.Subscription{Quotations: batch}); err != nil {
			return fmt.Errorf("subscribe quotations: %w", err)
		}
	}
	return nil
}

func (s *nativeSession) SecuritiesInfo(ctx context.Context, symbols []Symbol) ([]SecInfo, int, error) {
	out := make([]SecInfo, 0, len(symbols))
	for start := 0; start < len(symbols); start += nativeSecSpecBatch {
		end := start + nativeSecSpecBatch
		if end > len(symbols) {
			end = len(symbols)
		}
		ids := make([]uint16, 0, end-start)
		secs := make(map[uint16]records.SecNew, end-start)
		s.mu.RLock()
		for _, symbol := range symbols[start:end] {
			sec, ok := s.secBySymbol[symbol]
			if !ok {
				continue
			}
			ids = append(ids, sec.SecID)
			secs[sec.SecID] = sec
		}
		s.mu.RUnlock()
		if len(ids) == 0 {
			continue
		}
		payload, err := s.client.SecSpec(ctx, ids)
		if err != nil {
			if ctx.Err() != nil {
				return nil, 0, ctx.Err()
			}
			// Tolerate a bad chunk like the legacy per-security failures: the
			// missing ids are counted in failed and checkSecuritiesInfo decides
			// whether the catalog is usable at all.
			log.Warnf("request sec spec: %v", err)
			continue
		}
		specs, parseErr := records.ParseSecSpec(payload)
		if parseErr != nil {
			// A partially recovered mask must not take the trade feed down:
			// the parsed records still carry usable data and the missing ones
			// are counted as sec_info failures by the exporter core.
			log.Warnf("parse sec spec: %v", parseErr)
		}
		for _, spec := range specs {
			sec, ok := secs[spec.SecID]
			if !ok {
				continue
			}
			out = append(out, secInfoFromModel(spec.Info(sec)))
		}
	}
	return out, len(symbols) - len(out), nil
}

func (s *nativeSession) History(ctx context.Context, req HistoryRequest) ([]Candle, error) {
	candles, err := s.client.History(ctx, native.HistoryRequest{
		Symbol: model.Symbol{Board: req.Symbol.Board, SecCode: req.Symbol.SecCode},
		Period: req.Period,
		Count:  req.Count,
		Reset:  req.Reset,
	})
	if err != nil {
		return nil, err
	}
	return candlesFromModel(candles.Items), nil
}

func (s *nativeSession) Positions() []Symbol {
	positions := s.client.SecPositions()
	out := make([]Symbol, 0, len(positions))
	seen := make(map[Symbol]struct{}, len(positions))
	for _, position := range positions {
		symbol, ok := s.symbolByID(int(position.SecID))
		if !ok {
			continue
		}
		if _, exists := seen[symbol]; exists {
			continue
		}
		seen[symbol] = struct{}{}
		out = append(out, symbol)
	}
	return out
}

func (s *nativeSession) Run(ctx context.Context, handler func(Event) error) error {
	err := s.client.Run(ctx, native.RunOptions{}, func(events []model.Event) error {
		for _, event := range events {
			normalized, ok := s.normalize(event)
			if !ok {
				continue
			}
			if err := handler(normalized); err != nil {
				return err
			}
		}
		return nil
	})
	stats := s.client.Stats()
	log.Infof("TRANSAQ native stats: polls=%d trades=%d duplicates=%d unknown=%d",
		stats.Polls, stats.Trades, stats.Duplicates, stats.UnknownRecords)
	if ctx.Err() != nil {
		return nil
	}
	return err
}

func (s *nativeSession) Close() error {
	s.closeOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), nativeCloseTimeout)
		defer cancel()
		if err := s.client.Logout(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			log.Debugf("TRANSAQ native logout: %v", err)
		}
		s.closeErr = s.client.Close()
	})
	return s.closeErr
}

// addSecurities adds the securities the client reported after Dial and
// returns the ones the session did not know. It runs in the Run goroutine,
// the only one that mutates the client catalog, so the raw R_SEC_NEW lookup
// is safe here.
func (s *nativeSession) addSecurities(items []model.Security) []Security {
	raw := s.client.Catalog()
	s.mu.Lock()
	defer s.mu.Unlock()
	var added []Security
	for _, item := range items {
		if _, known := s.index.byID[item.SecID]; known {
			continue
		}
		sec := securityFromModel(item)
		s.catalog.Securities = append(s.catalog.Securities, sec)
		s.index.byID[sec.SecID] = sec
		s.index.bySymbol[sec.Symbol()] = sec
		if raw != nil {
			if rec, ok := raw.Security(uint16(item.SecID)); ok {
				s.secBySymbol[sec.Symbol()] = rec
			}
		}
		added = append(added, sec)
	}
	return added
}

func (s *nativeSession) symbolByID(secID int) (Symbol, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index.symbolByID(secID)
}

func (s *nativeSession) normalize(event model.Event) (Event, bool) {
	switch event := event.(type) {
	case *model.Trade:
		return TradeEvent{
			SecID:        int(event.SecID),
			Board:        event.Board,
			SecCode:      event.SecCode,
			TradeNo:      event.TradeNo,
			Time:         event.Time,
			Price:        event.Price.Float64(),
			Quantity:     clampUint32(event.Quantity),
			BuySell:      byteString(event.BuySell),
			OpenInterest: clampInt32(event.OpenInterest),
			Period:       byteString(event.Period),
		}, true
	case *model.Quotes:
		levels := make([]QuoteLevel, 0, len(event.Levels))
		for _, level := range event.Levels {
			yield := 0
			if level.Yield != nil {
				yield = *level.Yield
			}
			levels = append(levels, QuoteLevel{
				Price:  level.Price.Float64(),
				Source: level.Source,
				Yield:  yield,
				Buy:    level.Buy,
				Sell:   level.Sell,
			})
		}
		return QuotesEvent{
			SecID:   int(event.SecID),
			Board:   event.Board,
			SecCode: event.Symbol.SecCode,
			Time:    time.Now().In(moscowLocation),
			Levels:  levels,
		}, true
	case *model.Quotation:
		normalized := QuotationEvent{
			SecID:   int(event.SecID),
			Board:   event.Board,
			SecCode: event.Symbol.SecCode,
		}
		if event.Fields.Time != nil {
			normalized.Time = *event.Fields.Time
		}
		if event.Fields.Open != nil {
			normalized.Open = event.Fields.Open.Float64()
		}
		if event.Fields.Last != nil {
			normalized.Last = event.Fields.Last.Float64()
		}
		if event.Fields.Quantity != nil {
			normalized.Quantity = int(*event.Fields.Quantity)
		}
		return normalized, true
	case *model.SecuritiesAdded:
		added := s.addSecurities(event.Items)
		if len(added) == 0 {
			return nil, false
		}
		return CatalogAddedEvent{Securities: added}, true
	case *model.SecPosition:
		symbol, ok := s.symbolByID(int(event.SecID))
		if !ok {
			return nil, false
		}
		return PositionEvent{Symbol: symbol}, true
	case *model.SecInfoUpdate:
		symbol, ok := s.symbolByID(int(event.SecID))
		if !ok {
			return nil, false
		}
		return SecInfoUpdateEvent{Symbol: symbol}, true
	case model.ServerStatus:
		return StatusEvent{Connected: event.Connected, Recover: event.Recover, Err: event.Err}, true
	case model.UnknownRecord:
		return UnknownEvent{Tag: event.Tag, Offset: event.Offset}, true
	default:
		return nil, false
	}
}

func chunkSymbols(symbols []Symbol, size int) [][]model.Symbol {
	if len(symbols) == 0 {
		return nil
	}
	batches := make([][]model.Symbol, 0, (len(symbols)+size-1)/size)
	for start := 0; start < len(symbols); start += size {
		end := start + size
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := make([]model.Symbol, 0, end-start)
		for _, symbol := range symbols[start:end] {
			batch = append(batch, model.Symbol{Board: symbol.Board, SecCode: symbol.SecCode})
		}
		batches = append(batches, batch)
	}
	return batches
}

func securityFromModel(sec model.Security) Security {
	return Security{
		SecID:      sec.SecID,
		Active:     sec.Active,
		SecCode:    sec.SecCode,
		InstrClass: sec.InstrClass,
		Board:      sec.Board,
		Market:     sec.Market,
		ShortName:  sec.ShortName,
		Decimals:   sec.Decimals,
		MinStep:    sec.MinStep,
		LotSize:    sec.LotSize,
		LotDivider: sec.LotDivider,
		PointCost:  sec.PointCost,
		SecType:    sec.SecType,
		QuotesType: sec.QuotesType,
	}
}

func secInfoFromModel(info model.SecInfo) SecInfo {
	out := SecInfo{
		SecID:         int(info.SecID),
		SecName:       info.SecName,
		SecCode:       info.SecCode,
		Market:        info.Market,
		PName:         info.PName,
		MatDate:       copyTime(info.MatDate),
		ClearingPrice: decimalValue(info.ClearingPrice),
		MinPrice:      decimalValue(info.MinPrice),
		MaxPrice:      decimalValue(info.MaxPrice),
		BuyDeposit:    decimalValue(info.BuyDeposit),
		SellDeposit:   decimalValue(info.SellDeposit),
		BgoC:          decimalValue(info.BgoC),
		BgoNC:         decimalValue(info.BgoNC),
		BgoBuy:        decimalValue(info.BgoBuy),
		AccruedInt:    decimalValue(info.AccruedInt),
		CouponValue:   decimalValue(info.CouponValue),
		CouponDate:    copyTime(info.CouponDate),
		FaceValue:     decimalValue(info.FaceValue),
		PutCall:       stringValue(info.PutCall),
		PointCost:     decimalValue(info.PointCost),
		OptType:       stringValue(info.OptType),
		ISIN:          info.ISIN,
		RegNumber:     info.RegNumber,
		BuybackPrice:  decimalValue(info.BuybackPrice),
		BuybackDate:   copyTime(info.BuybackDate),
		CurrencyID:    info.CurrencyID,
	}
	if info.CouponPeriod != nil {
		out.CouponPeriod = int(*info.CouponPeriod)
	}
	if info.LotVolume != nil {
		out.LotVolume = int(*info.LotVolume)
	}
	return out
}

func candlesFromModel(items []model.Candle) []Candle {
	out := make([]Candle, 0, len(items))
	for _, candle := range items {
		out = append(out, Candle{
			Time:   candle.Time,
			Open:   candle.Open.Float64(),
			Close:  candle.Close.Float64(),
			High:   candle.High.Float64(),
			Low:    candle.Low.Float64(),
			Volume: candle.Volume,
		})
	}
	return out
}

func decimalValue(value *model.Decimal) float64 {
	if value == nil {
		return 0
	}
	return value.Float64()
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func copyTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}

func byteString(value byte) string {
	if value == 0 {
		return ""
	}
	return string([]byte{value})
}
