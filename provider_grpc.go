package main

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	tcClient "github.com/kmlebedev/txmlconnector/client"
	"github.com/kmlebedev/txmlconnector/client/commands"
	pb "github.com/kmlebedev/txmlconnector/proto"
	log "github.com/sirupsen/logrus"
)

const (
	grpcPortfolioBuffer = 4096
	grpcCatalogQuiet    = 5 * time.Second
	grpcCatalogTimeout  = 2 * time.Minute
	grpcCommandTimeout  = 30 * time.Second
	grpcSecInfoTimeout  = 15 * time.Second
	grpcHistoryTimeout  = 5 * time.Minute
	grpcCloseTimeout    = 2 * time.Second
)

var (
	errResponseStreamClosed = errors.New("txmlconnector response stream closed")
	errCommandFailed        = errors.New("TRANSAQ command failed")
)

// newGRPCClient is replaceable in tests: the legacy provider must be testable
// without a txmlconnector-server.
var newGRPCClient = func(options tcClient.Options) (*tcClient.TCClient, error) {
	return tcClient.NewTCClientWithOptions(options)
}

type grpcProvider struct {
	options tcClient.Options
}

func (p *grpcProvider) Name() string { return backendGRPC }

func (p *grpcProvider) Open(ctx context.Context) (Session, error) {
	client, err := newGRPCClient(p.options)
	if err != nil {
		return nil, fmt.Errorf("create gRPC client: %w", err)
	}
	session := newGRPCSession(client)
	session.start()
	if err := session.waitReady(ctx); err != nil {
		_ = session.Close()
		return nil, err
	}
	return session, nil
}

func newGRPCProvider() (Provider, error) {
	login := os.Getenv("TC_LOGIN")
	password := os.Getenv("TC_PASSWORD")
	host := os.Getenv("TC_HOST")
	port := os.Getenv("TC_PORT")
	switch {
	case login == "":
		return nil, errors.New("TC_LOGIN is required for the grpc provider")
	case password == "":
		return nil, errors.New("TC_PASSWORD is required for the grpc provider")
	case host == "":
		return nil, errors.New("TC_HOST is required for the grpc provider")
	case port == "":
		return nil, errors.New("TC_PORT is required for the grpc provider")
	}
	return &grpcProvider{options: tcClient.Options{
		Target:          os.Getenv("TC_TARGET"),
		PortfolioBuffer: grpcPortfolioBuffer,
		Connect: &tcClient.ConnectConfig{
			Login:          login,
			Password:       password,
			Host:           host,
			Port:           port,
			SessionTimeout: 60,
			RequestTimeout: 10,
			PushUlimits:    30,
			PushPosEquity:  30,
		},
	}}, nil
}

func init() { RegisterProvider(backendGRPC, newGRPCProvider) }

// securitiesCatalog accumulates the <securities> chunks of the legacy client:
// TCClient.Data.Securities keeps only the last message, so the subscription
// must be built from every chunk seen in the session.
type securitiesCatalog struct {
	mu     sync.Mutex
	order  []Security
	seen   map[int]struct{}
	lastAt time.Time
}

func newSecuritiesCatalog() *securitiesCatalog {
	return &securitiesCatalog{seen: make(map[int]struct{})}
}

func (c *securitiesCatalog) add(items []Security) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, item := range items {
		if _, ok := c.seen[item.SecID]; ok {
			continue
		}
		c.seen[item.SecID] = struct{}{}
		c.order = append(c.order, item)
	}
	c.lastAt = time.Now()
}

// reset clears the catalog before a new session: Transaq numbers secids within
// a server session, so a subscription from the previous session may point at
// other instruments after a broker restart.
func (c *securitiesCatalog) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.order = nil
	clear(c.seen)
	c.lastAt = time.Time{}
}

func (c *securitiesCatalog) snapshot() []Security {
	c.mu.Lock()
	defer c.mu.Unlock()
	items := make([]Security, len(c.order))
	copy(items, c.order)
	return items
}

func (c *securitiesCatalog) size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.order)
}

func (c *securitiesCatalog) lastUpdate() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastAt
}

// waitQuiet waits until the catalog stream calms down: the last chunk arrived
// at least quiet ago. An empty catalog is an error: subscribing to it would send
// an empty <alltrades>.
func (c *securitiesCatalog) waitQuiet(ctx context.Context, quiet, timeout time.Duration) error {
	if quiet <= 0 {
		return nil
	}
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(quiet / 5)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case now := <-ticker.C:
			if c.size() == 0 {
				if now.After(deadline) {
					return fmt.Errorf("каталог securities не пришёл за %s: подписка была бы пустой", timeout)
				}
				continue
			}
			if now.Sub(c.lastUpdate()) >= quiet {
				return nil
			}
			if now.After(deadline) {
				// The catalog is non-empty: subscribe to what was seen instead
				// of failing the session.
				return nil
			}
		}
	}
}

type historyKey struct {
	secID  int
	period int
}

type grpcHistoryWaiter struct {
	ch    chan commands.Candles
	parts []commands.Candle
}

type grpcSession struct {
	tc      *tcClient.TCClient
	catalog *securitiesCatalog
	events  *eventQueue
	ready   chan error
	done    chan struct{}

	closeOnce sync.Once
	mu        sync.Mutex
	terminal  error

	candleKinds    []CandleKind
	secIDs         map[Symbol]int
	secsByID       map[int]Security
	secInfoWaiters map[int]chan commands.SecInfo
	historyWaiters map[historyKey]*grpcHistoryWaiter
	positions      map[int]Symbol
}

func newGRPCSession(client *tcClient.TCClient) *grpcSession {
	return &grpcSession{
		tc:             client,
		catalog:        newSecuritiesCatalog(),
		events:         newEventQueue(),
		ready:          make(chan error, 1),
		done:           make(chan struct{}),
		secIDs:         make(map[Symbol]int),
		secsByID:       make(map[int]Security),
		secInfoWaiters: make(map[int]chan commands.SecInfo),
		historyWaiters: make(map[historyKey]*grpcHistoryWaiter),
		positions:      make(map[int]Symbol),
	}
}

func (s *grpcSession) start() {
	s.startWithUpdates(s.tc.PortfolioUpdates())
}

func (s *grpcSession) startWithUpdates(updates <-chan tcClient.PortfolioUpdate) {
	s.catalog.reset()
	s.mu.Lock()
	clear(s.secIDs)
	clear(s.secsByID)
	s.mu.Unlock()
	if updates != nil {
		go s.consumeCatalog(updates)
	}
	go s.pump()
}

func (s *grpcSession) consumeCatalog(updates <-chan tcClient.PortfolioUpdate) {
	for {
		select {
		case <-s.done:
			return
		case update, ok := <-updates:
			if !ok {
				return
			}
			if update.Kind != tcClient.UpdateSecurities || update.Securities == nil {
				continue
			}
			s.addSecurities(securitiesFromGRPC(update.Securities.Items))
		}
	}
}

func (s *grpcSession) pump() {
	for {
		select {
		case <-s.done:
			return
		case status := <-s.tc.ServerStatusChan:
			if !s.handleStatus(status) {
				return
			}
		case trades := <-s.tc.AllTradesChan:
			s.pushTrades(trades)
		case quotes := <-s.tc.QuotesChan:
			s.pushQuotes(quotes)
		case secInfo := <-s.tc.SecInfoChan:
			s.handleSecInfo(secInfo)
		case update := <-s.tc.SecInfoUpdChan:
			s.pushSecInfoUpdate(update)
		case response := <-s.tc.ResponseChannel:
			s.handleResponse(response)
		case <-s.tc.ShutdownChannel:
			s.setTerminal(errResponseStreamClosed)
			return
		}
	}
}

func (s *grpcSession) handleStatus(status commands.ServerStatus) bool {
	switch status.Connected {
	case "true":
		s.captureCandleKinds()
		s.events.Push(StatusEvent{Connected: true, Recover: status.Recover == "true"})
		s.signalReady(nil)
		return true
	case "false", "error":
		err := fmt.Errorf("TRANSAQ terminal is not connected: %+v", status)
		s.events.Push(StatusEvent{Connected: false, Err: err.Error()})
		s.setTerminal(err)
		return false
	default:
		log.Infof("Status %+v", status)
		return true
	}
}

func (s *grpcSession) captureCandleKinds() {
	s.mu.Lock()
	s.candleKinds = candleKindsFromGRPC(s.tc.Data.CandleKinds.Items)
	s.mu.Unlock()
}

func (s *grpcSession) handleResponse(response string) {
	switch response {
	case "candles":
		s.handleCandles()
	case "quotations":
		s.pushQuotations()
	case "positions":
		s.handlePositions()
	case "united_portfolio":
		log.Infof("UnitedPortfolio: ```\n%+v\n```", s.tc.Data.UnitedPortfolio)
	case "united_equity":
		log.Infof("UnitedEquity: ```\n%+v\n```", s.tc.Data.UnitedEquity)
	default:
		log.Debugf("receive %s", response)
	}
}

func (s *grpcSession) pushTrades(trades commands.AllTrades) {
	for _, trade := range trades.Items {
		event, err := tradeEventFromGRPC(trade)
		if err != nil {
			log.Errorf("TRANSAQ trade: %v", err)
			continue
		}
		s.events.Push(event)
	}
}

func (s *grpcSession) pushQuotes(quotes commands.Quotes) {
	levels := make([]QuoteLevel, 0, len(quotes.Items))
	for _, quote := range quotes.Items {
		levels = append(levels, QuoteLevel{
			Price:  quote.Price,
			Source: quote.Source,
			Yield:  quote.Yield,
			Buy:    int64(quote.Buy),
			Sell:   int64(quote.Sell),
		})
	}
	if len(levels) == 0 {
		return
	}
	first := quotes.Items[0]
	s.events.Push(QuotesEvent{
		SecID:   first.SecId,
		Board:   first.Board,
		SecCode: first.SecCode,
		Time:    quotes.Time,
		Levels:  levels,
	})
}

func (s *grpcSession) pushQuotations() {
	quotations := s.tc.Data.Quotations
	for _, quotation := range quotations.Items {
		s.events.Push(QuotationEvent{
			SecID:    quotation.SecId,
			Board:    quotation.Board,
			SecCode:  quotation.SecCode,
			Time:     quotation.Time,
			Open:     quotation.Open,
			Last:     quotation.Last,
			Quantity: quotation.Quantity,
		})
	}
}

func (s *grpcSession) handleCandles() {
	data := s.tc.Data.Candles
	items := append([]commands.Candle(nil), data.Items...)
	key := historyKey{secID: data.SecId, period: data.Period}

	s.mu.Lock()
	waiter := s.historyWaiters[key]
	if waiter != nil && data.Status != 2 {
		delete(s.historyWaiters, key)
	}
	s.mu.Unlock()

	if waiter == nil {
		s.events.Push(CandlesEvent{
			SecID:   data.SecId,
			Board:   data.Board,
			SecCode: data.SecCode,
			Period:  data.Period,
			Items:   candlesFromGRPC(items),
		})
		return
	}
	waiter.parts = append(waiter.parts, items...)
	if data.Status == 2 {
		return
	}
	select {
	case waiter.ch <- commands.Candles{
		SecId:   data.SecId,
		SecCode: data.SecCode,
		Period:  data.Period,
		Status:  data.Status,
		Items:   waiter.parts,
	}:
	default:
	}
}

func (s *grpcSession) handleSecInfo(info commands.SecInfo) {
	s.mu.Lock()
	waiter := s.secInfoWaiters[info.SecId]
	if waiter != nil {
		delete(s.secInfoWaiters, info.SecId)
	}
	s.mu.Unlock()
	if waiter == nil {
		log.Debugf("receive sec_info %d without a waiter", info.SecId)
		return
	}
	select {
	case waiter <- info:
	default:
	}
}

func (s *grpcSession) handlePositions() {
	data := s.tc.Data.Positions
	if data == nil {
		return
	}
	items := append([]commands.SecPosition(nil), data.SecPositions...)
	symbols := make([]Symbol, 0, len(items))

	s.mu.Lock()
	for _, position := range items {
		sec, ok := s.secsByID[position.SecId]
		if !ok {
			continue
		}
		s.positions[position.SecId] = sec.Symbol()
		symbols = append(symbols, sec.Symbol())
	}
	s.mu.Unlock()

	for _, symbol := range symbols {
		s.events.Push(PositionEvent{Symbol: symbol})
	}
	log.Infof("Positions: %d securities, %d money", len(items), len(data.MoneyPosition))
}

func (s *grpcSession) pushSecInfoUpdate(update commands.SecInfoUpd) {
	s.mu.Lock()
	sec, ok := s.secsByID[update.SecId]
	s.mu.Unlock()
	if !ok {
		return
	}
	s.events.Push(SecInfoUpdateEvent{Symbol: sec.Symbol()})
}

func (s *grpcSession) setTerminal(err error) {
	s.mu.Lock()
	if s.terminal == nil {
		s.terminal = err
	}
	s.mu.Unlock()
	s.events.Close()
	s.signalReady(err)
}

func (s *grpcSession) signalReady(err error) {
	select {
	case s.ready <- err:
	default:
	}
}

func (s *grpcSession) waitReady(ctx context.Context) error {
	select {
	case err := <-s.ready:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return errResponseStreamClosed
	}
	if s.tc.PortfolioUpdates() != nil {
		return s.catalog.waitQuiet(ctx, grpcCatalogQuiet, grpcCatalogTimeout)
	}
	// Compatibility path without the typed portfolio stream: the last
	// Data.Securities snapshot is the catalog (the old updateSecurities fallback).
	s.addSecurities(securitiesFromGRPC(s.tc.Data.Securities.Items))
	if s.catalog.size() == 0 {
		return fmt.Errorf("TRANSAQ catalog is empty: no securities received")
	}
	return nil
}

func (s *grpcSession) Catalog() Catalog {
	s.mu.Lock()
	kinds := append([]CandleKind(nil), s.candleKinds...)
	s.mu.Unlock()
	return Catalog{Securities: s.catalog.snapshot(), CandleKinds: kinds}
}

func (s *grpcSession) Subscribe(ctx context.Context, sub Subscription) error {
	allTrades := make([]int, 0, len(sub.AllTrades))
	quotations := make([]commands.SubSecurity, 0, len(sub.Quotations))
	s.mu.Lock()
	for _, symbol := range sub.AllTrades {
		secID, ok := s.secIDs[symbol]
		if !ok {
			s.mu.Unlock()
			return fmt.Errorf("unknown security %s", symbol)
		}
		allTrades = append(allTrades, secID)
	}
	for _, symbol := range sub.Quotations {
		secID, ok := s.secIDs[symbol]
		if !ok {
			s.mu.Unlock()
			return fmt.Errorf("unknown security %s", symbol)
		}
		quotations = append(quotations, commands.SubSecurity{SecId: secID})
	}
	s.mu.Unlock()
	if len(allTrades) == 0 && len(quotations) == 0 {
		return nil
	}
	_, err := s.sendCommand(ctx, commands.Command{
		Id:         "subscribe",
		AllTrades:  commands.SubAllTrades{Items: allTrades},
		Quotations: quotations,
	})
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	log.Infof("Subscribe trades %d, quotations %d", len(allTrades), len(quotations))
	return nil
}

func (s *grpcSession) SecuritiesInfo(ctx context.Context, symbols []Symbol) ([]SecInfo, int, error) {
	pending := make([]int, 0, len(symbols))
	waiters := make(map[int]chan commands.SecInfo, len(symbols))
	failed := 0
	s.mu.Lock()
	ids := make([]int, len(symbols))
	found := make([]bool, len(symbols))
	for i, symbol := range symbols {
		ids[i], found[i] = s.secIDs[symbol]
	}
	s.mu.Unlock()
	for i, found := range found {
		if !found {
			failed++
			continue
		}
		secID := ids[i]
		waiter := make(chan commands.SecInfo, 1)
		waiters[secID] = waiter
		s.mu.Lock()
		s.secInfoWaiters[secID] = waiter
		s.mu.Unlock()
		message := fmt.Sprintf(`<command id="get_securities_info"><secid>%d</secid></command>`, secID)
		if _, err := s.sendMessage(ctx, message); err != nil {
			s.removeSecInfoWaiter(secID)
			if ctx.Err() != nil {
				return nil, failed, ctx.Err()
			}
			if !errors.Is(err, errCommandFailed) {
				// Transport failures are session-level: retrying thousands of
				// per-bond commands on a dead stream would only spin.
				return nil, failed, err
			}
			failed++
			log.Warnf("get securities info for %d: %v", secID, err)
			continue
		}
		pending = append(pending, secID)
	}

	deadline := time.Now().Add(grpcSecInfoTimeout)
	out := make([]SecInfo, 0, len(pending))
	timeouts := 0
	for index, secID := range pending {
		waiter := waiters[secID]
		remaining := time.Until(deadline)
		if remaining <= 0 {
			for _, rest := range pending[index:] {
				s.removeSecInfoWaiter(rest)
			}
			timeouts += len(pending) - index
			break
		}
		timer := time.NewTimer(remaining)
		select {
		case info := <-waiter:
			timer.Stop()
			out = append(out, secInfoFromCommands(info))
		case <-timer.C:
			// A timeout is not a definitive command failure: the old exporter
			// inserted late sec_info callbacks as they arrived. Do not feed it
			// to checkSecuritiesInfo, or a slow bulk request would restart the
			// whole session forever.
			s.removeSecInfoWaiter(secID)
			timeouts++
		case <-ctx.Done():
			timer.Stop()
			s.removeSecInfoWaiter(secID)
			return nil, failed, ctx.Err()
		}
	}
	if timeouts > 0 {
		log.Warnf("sec_info: %d of %d responses timed out and were not written", timeouts, len(symbols))
	}
	return out, failed, nil
}

func (s *grpcSession) removeSecInfoWaiter(secID int) {
	s.mu.Lock()
	delete(s.secInfoWaiters, secID)
	s.mu.Unlock()
}

func (s *grpcSession) History(ctx context.Context, req HistoryRequest) ([]Candle, error) {
	s.mu.Lock()
	secID, ok := s.secIDs[req.Symbol]
	s.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("unknown security %s", req.Symbol)
	}
	key := historyKey{secID: secID, period: req.Period}
	waiter := &grpcHistoryWaiter{ch: make(chan commands.Candles, 1)}
	s.mu.Lock()
	s.historyWaiters[key] = waiter
	s.mu.Unlock()
	defer s.removeHistoryWaiter(key)

	_, err := s.sendCommand(ctx, commands.Command{
		Id:     "gethistorydata",
		SecId:  secID,
		Period: req.Period,
		Count:  req.Count,
		Reset:  strconv.FormatBool(req.Reset),
	})
	if err != nil {
		return nil, fmt.Errorf("gethistorydata %s: %w", req.Symbol, err)
	}
	timer := time.NewTimer(grpcHistoryTimeout)
	defer timer.Stop()
	select {
	case data := <-waiter.ch:
		if data.SecId != secID || data.Period != req.Period {
			return nil, fmt.Errorf("gethistorydata %s: response for secid %d period %d", req.Symbol, data.SecId, data.Period)
		}
		return candlesFromGRPC(data.Items), nil
	case <-timer.C:
		return nil, fmt.Errorf("gethistorydata %s period %d: timeout", req.Symbol, req.Period)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *grpcSession) removeHistoryWaiter(key historyKey) {
	s.mu.Lock()
	delete(s.historyWaiters, key)
	s.mu.Unlock()
}

func (s *grpcSession) Positions() []Symbol {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Symbol, 0, len(s.positions))
	for _, symbol := range s.positions {
		out = append(out, symbol)
	}
	return out
}

func (s *grpcSession) Run(ctx context.Context, handler func(Event) error) error {
	for {
		event, ok := s.events.Pop(ctx)
		if !ok {
			break
		}
		if err := handler(event); err != nil {
			return err
		}
	}
	if ctx.Err() != nil {
		return nil
	}
	s.mu.Lock()
	err := s.terminal
	s.mu.Unlock()
	if err == nil {
		err = errResponseStreamClosed
	}
	return err
}

func (s *grpcSession) Close() error {
	s.closeOnce.Do(func() {
		close(s.done)
		s.events.Close()
	})
	ctx, cancel := context.WithTimeout(context.Background(), grpcCloseTimeout)
	defer cancel()
	if s.tc.Client != nil {
		if _, err := s.sendCommand(ctx, commands.Command{Id: "disconnect"}); err != nil {
			log.Debugf("Disconnect TRANSAQ gRPC client: %v", err)
		}
	}
	s.tc.Close()
	return nil
}

func (s *grpcSession) sendCommand(ctx context.Context, command interface{}) (string, error) {
	message, err := commands.EncodeRequestE(command)
	if err != nil {
		return "", fmt.Errorf("encode command: %w", err)
	}
	return s.sendMessage(ctx, message)
}

func (s *grpcSession) sendMessage(ctx context.Context, message string) (string, error) {
	if s.tc.Client == nil {
		return "", errors.New("gRPC client is not configured")
	}
	callCtx, cancel := context.WithTimeout(ctx, grpcCommandTimeout)
	defer cancel()
	response, err := s.tc.Client.SendCommand(callCtx, &pb.SendCommandRequest{Message: message})
	if err != nil {
		return "", fmt.Errorf("send command: %w", err)
	}
	result := commands.Result{}
	if err := xml.Unmarshal([]byte(response.GetMessage()), &result); err != nil {
		return "", fmt.Errorf("decode command response %q: %w", response.GetMessage(), err)
	}
	if result.Success != "true" {
		return response.GetMessage(), fmt.Errorf("%w: %s", errCommandFailed, result.Message)
	}
	return response.GetMessage(), nil
}

// addSecurities keeps the accumulator and the lookup indexes in one critical
// section: a reader must never see a security in Catalog() but miss it in
// secIDs (Subscribe would reject a known symbol) or vice versa.
func (s *grpcSession) addSecurities(items []Security) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.catalog.add(items)
	for _, sec := range items {
		s.secIDs[sec.Symbol()] = sec.SecID
		s.secsByID[sec.SecID] = sec
	}
}

func securitiesFromGRPC(items []commands.Security) []Security {
	out := make([]Security, 0, len(items))
	for _, item := range items {
		out = append(out, Security{
			SecID:      item.SecId,
			Active:     item.Active == "true",
			SecCode:    item.SecCode,
			InstrClass: item.InstrClass,
			Board:      item.Board,
			Market:     item.Market,
			ShortName:  item.ShortName,
			Decimals:   item.Decimals,
			MinStep:    item.MinStep,
			LotSize:    item.LotSize,
			LotDivider: item.LotDivider,
			PointCost:  item.PointCost,
			SecType:    item.SecType,
			QuotesType: item.QuotesType,
		})
	}
	return out
}

func candleKindsFromGRPC(items []commands.CandleKind) []CandleKind {
	out := make([]CandleKind, 0, len(items))
	for _, item := range items {
		out = append(out, CandleKind{ID: item.ID, Period: item.Period, Name: item.Name})
	}
	return out
}

func tradeEventFromGRPC(trade commands.Trade) (TradeEvent, error) {
	tradeTime, err := time.ParseInLocation(tradeTimeLayout, trade.Time, moscowLocation)
	if err != nil {
		return TradeEvent{}, fmt.Errorf("trade %d: parse time %q: %w", trade.TradeNo, trade.Time, err)
	}
	return TradeEvent{
		SecID:        trade.SecId,
		Board:        trade.Board,
		SecCode:      trade.SecCode,
		TradeNo:      trade.TradeNo,
		Time:         tradeTime,
		Price:        trade.Price,
		Quantity:     clampUint32(int64(trade.Quantity)),
		BuySell:      trade.BuySell,
		OpenInterest: clampInt32(int64(trade.OpenInterest)),
		Period:       trade.Period,
	}, nil
}

func candlesFromGRPC(items []commands.Candle) []Candle {
	out := make([]Candle, 0, len(items))
	for _, candle := range items {
		candleTime, err := time.ParseInLocation(tradeTimeLayout, candle.Date, moscowLocation)
		if err != nil {
			log.Warnf("TRANSAQ candle %q: parse time: %v", candle.Date, err)
			continue
		}
		out = append(out, Candle{
			Time:   candleTime,
			Open:   candle.Open,
			Close:  candle.Close,
			High:   candle.High,
			Low:    candle.Low,
			Volume: candle.Volume,
		})
	}
	return out
}
