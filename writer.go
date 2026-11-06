package main

import (
	"context"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	tradeBatchSize     = 1000
	tradeFlushInterval = 500 * time.Millisecond
	dispatcherBuffer   = 256
	positionQueueSize  = 64
	positionSubTimeout = 30 * time.Second
	finalFlushTimeout  = 10 * time.Second
)

// tradeBatcher collects individual native trades into ClickHouse batches: the
// native client delivers one trade per event, and one insert per trade would be
// both slow and hard on the server.
type tradeBatcher struct {
	ctx   context.Context
	stop  chan struct{}
	done  chan struct{}
	once  sync.Once
	mu    sync.Mutex
	queue []tradeRow
	wake  chan struct{}
	flush time.Duration
}

func newTradeBatcher(ctx context.Context, flush time.Duration) *tradeBatcher {
	batcher := &tradeBatcher{
		ctx:   ctx,
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
		wake:  make(chan struct{}, 1),
		flush: flush,
	}
	go batcher.run()
	return batcher
}

func (b *tradeBatcher) Write(event TradeEvent) {
	b.mu.Lock()
	select {
	case <-b.stop:
		b.mu.Unlock()
		return
	default:
	}
	b.queue = append(b.queue, tradeRowFromEvent(event))
	b.mu.Unlock()
	select {
	case b.wake <- struct{}{}:
	default:
	}
}

func (b *tradeBatcher) run() {
	defer close(b.done)
	ticker := time.NewTicker(b.flush)
	defer ticker.Stop()
	for {
		select {
		case <-b.ctx.Done():
			b.flushPending(context.WithoutCancel(b.ctx))
			return
		case <-b.stop:
			b.flushPending(context.WithoutCancel(b.ctx))
			return
		case <-b.wake:
			b.flushBatch()
		case <-ticker.C:
			b.flushPending(b.ctx)
		}
	}
}

func (b *tradeBatcher) flushBatch() {
	for {
		b.mu.Lock()
		if len(b.queue) < tradeBatchSize {
			b.mu.Unlock()
			return
		}
		rows := b.takeLocked(tradeBatchSize)
		b.mu.Unlock()
		insertCtx, cancel := context.WithTimeout(b.ctx, finalFlushTimeout)
		err := insertTradeRows(insertCtx, rows)
		cancel()
		if err != nil && b.ctx.Err() == nil {
			log.Errorf("Insert TRANSAQ trades: %v", err)
		}
	}
}

func (b *tradeBatcher) flushPending(ctx context.Context) {
	for {
		b.mu.Lock()
		rows := b.takeLocked(tradeBatchSize)
		b.mu.Unlock()
		if len(rows) == 0 {
			return
		}
		insertCtx, cancel := context.WithTimeout(ctx, finalFlushTimeout)
		err := insertTradeRows(insertCtx, rows)
		cancel()
		if err != nil {
			log.Errorf("Flush TRANSAQ trades: %v", err)
		}
	}
}

func (b *tradeBatcher) takeLocked(limit int) []tradeRow {
	if len(b.queue) == 0 {
		return nil
	}
	if limit > len(b.queue) {
		limit = len(b.queue)
	}
	rows := make([]tradeRow, limit)
	copy(rows, b.queue[:limit])
	var zero tradeRow
	for i := 0; i < limit; i++ {
		b.queue[i] = zero
	}
	b.queue = b.queue[limit:]
	return rows
}

func (b *tradeBatcher) Close() {
	b.once.Do(func() { close(b.stop) })
	<-b.done
}

// eventDispatcher owns the per-session ClickHouse writers. Provider events are
// enqueued here, so the provider poll loop never waits for ClickHouse.
type eventDispatcher struct {
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	cfg        exportConfig
	trades     *tradeBatcher
	quotes     chan QuotesEvent
	quotations chan QuotationEvent
	candles    chan CandlesEvent
	positions  *positionSubscriber
}

func newEventDispatcher(parent context.Context, cfg exportConfig, sub Subscriber, subscribed []Symbol) *eventDispatcher {
	ctx, cancel := context.WithCancel(parent)
	dispatcher := &eventDispatcher{
		ctx:        ctx,
		cancel:     cancel,
		cfg:        cfg,
		trades:     newTradeBatcher(ctx, tradeFlushInterval),
		quotes:     make(chan QuotesEvent, dispatcherBuffer),
		quotations: make(chan QuotationEvent, dispatcherBuffer),
		candles:    make(chan CandlesEvent, dispatcherBuffer),
	}
	startQueuedWorker(ctx, &dispatcher.wg, "quotes", dispatcher.quotes, func(ctx context.Context, event QuotesEvent) error {
		return insertQuoteRows(ctx, quoteRowsFromEvent(event))
	})
	aggregator := newQuotationAggregator()
	startQueuedWorker(ctx, &dispatcher.wg, "quotations", dispatcher.quotations, func(ctx context.Context, event QuotationEvent) error {
		return insertCandleRows(ctx, aggregator.Apply(event, time.Now()))
	})
	startQueuedWorker(ctx, &dispatcher.wg, "candles", dispatcher.candles, func(ctx context.Context, event CandlesEvent) error {
		return insertCandleRows(ctx, candleRowsFromEvent(event))
	})
	if sub != nil {
		dispatcher.positions = newPositionSubscriber(ctx, sub, subscribed)
	}
	return dispatcher
}

func (d *eventDispatcher) Handle(event Event) error {
	switch event := event.(type) {
	case TradeEvent:
		d.trades.Write(event)
	case QuotesEvent:
		d.sendQuotes(event)
	case QuotationEvent:
		d.sendQuotations(event)
	case CandlesEvent:
		d.sendCandles(event)
	case PositionEvent:
		if d.cfg.Positions && d.positions != nil {
			d.positions.Add(event.Symbol)
		}
	case SecInfoUpdateEvent:
		log.Debugf("secInfoUpd %s", event.Symbol)
	case StatusEvent:
		logStatusEvent(event)
	case UnknownEvent:
		log.Warnf("TRANSAQ unknown record tag %#02x at offset %d", event.Tag, event.Offset)
	default:
		log.Debugf("TRANSAQ event %T ignored", event)
	}
	return nil
}

func (d *eventDispatcher) sendQuotes(event QuotesEvent) {
	select {
	case d.quotes <- event:
	case <-d.ctx.Done():
	}
}

func (d *eventDispatcher) sendQuotations(event QuotationEvent) {
	select {
	case d.quotations <- event:
	case <-d.ctx.Done():
	}
}

func (d *eventDispatcher) sendCandles(event CandlesEvent) {
	select {
	case d.candles <- event:
	case <-d.ctx.Done():
	}
}

// WriteCandles is used by the history workers, which do not come through the
// provider event stream.
func (d *eventDispatcher) WriteCandles(secCode string, period int, items []Candle) {
	if len(items) == 0 {
		return
	}
	d.sendCandles(CandlesEvent{SecCode: secCode, Period: period, Items: items})
}

func (d *eventDispatcher) Close() {
	d.cancel()
	if d.positions != nil {
		d.positions.Close()
	}
	d.wg.Wait()
	d.trades.Close()
}

func logStatusEvent(event StatusEvent) {
	switch {
	case event.Err != "":
		log.Warnf("TRANSAQ session status: %s", event.Err)
	case event.Recover:
		log.Warnf("TRANSAQ connection lost, recovering")
	case event.Connected:
		log.Infof("TRANSAQ connected")
	default:
		log.Infof("TRANSAQ session status: %+v", event)
	}
}

func tradeRowFromEvent(event TradeEvent) tradeRow {
	return tradeRow{
		Time:         event.Time,
		SecID:        event.SecID,
		SecCode:      event.SecCode,
		TradeNo:      event.TradeNo,
		Board:        event.Board,
		Price:        event.Price,
		Quantity:     event.Quantity,
		BuySell:      event.BuySell,
		OpenInterest: event.OpenInterest,
		Period:       event.Period,
	}
}

func quoteRowsFromEvent(event QuotesEvent) []quoteRow {
	rows := make([]quoteRow, 0, len(event.Levels))
	for _, level := range event.Levels {
		rows = append(rows, quoteRow{
			Time:    event.Time,
			SecID:   event.SecID,
			Board:   event.Board,
			SecCode: event.SecCode,
			Price:   level.Price,
			Source:  level.Source,
			Yield:   clampInt8(int64(level.Yield)),
			Buy:     clampInt16(level.Buy),
			Sell:    clampInt16(level.Sell),
		})
	}
	return rows
}

func candleRowsFromEvent(event CandlesEvent) []candleRow {
	return candleRows(event.SecCode, event.Period, event.Items)
}

func candleRows(secCode string, period int, items []Candle) []candleRow {
	rows := make([]candleRow, 0, len(items))
	for _, candle := range items {
		rows = append(rows, candleRow{
			Time:    candle.Time,
			SecCode: secCode,
			Period:  uint16(period),
			Open:    candle.Open,
			Close:   candle.Close,
			High:    candle.High,
			Low:     candle.Low,
			Volume:  clampUint64(candle.Volume),
		})
	}
	return rows
}

// positionSubscriber subscribes the alltrades feed of a symbol that appeared in
// the position stream after the session start. Failures are logged: the next
// session rebuilds the plan from the loaded positions.
type positionSubscriber struct {
	sub  Subscriber
	ctx  context.Context
	in   chan Symbol
	stop chan struct{}
	once sync.Once
	wg   sync.WaitGroup
	mu   sync.Mutex
	seen map[Symbol]struct{}
}

func newPositionSubscriber(ctx context.Context, sub Subscriber, initial []Symbol) *positionSubscriber {
	subscriber := &positionSubscriber{
		sub:  sub,
		ctx:  ctx,
		in:   make(chan Symbol, positionQueueSize),
		stop: make(chan struct{}),
		seen: make(map[Symbol]struct{}, len(initial)),
	}
	for _, symbol := range initial {
		subscriber.seen[symbol] = struct{}{}
	}
	subscriber.wg.Add(1)
	go subscriber.run()
	return subscriber
}

func (p *positionSubscriber) Add(symbol Symbol) {
	p.mu.Lock()
	if _, exists := p.seen[symbol]; exists {
		p.mu.Unlock()
		return
	}
	p.seen[symbol] = struct{}{}
	p.mu.Unlock()
	select {
	case p.in <- symbol:
		return
	case <-p.ctx.Done():
	case <-p.stop:
	default:
	}
	// The queue is full (or the worker is gone): forget the mark so a later
	// position event can retry the subscription.
	p.mu.Lock()
	delete(p.seen, symbol)
	p.mu.Unlock()
	log.Warnf("TRANSAQ position subscription queue is full; skipping %s", symbol)
}

func (p *positionSubscriber) run() {
	defer p.wg.Done()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-p.stop:
			return
		case symbol := <-p.in:
			ctx, cancel := context.WithTimeout(p.ctx, positionSubTimeout)
			err := p.sub.Subscribe(ctx, Subscription{AllTrades: []Symbol{symbol}})
			cancel()
			if err != nil && p.ctx.Err() == nil {
				log.Warnf("Subscribe position %s: %v", symbol, err)
			}
		}
	}
}

func (p *positionSubscriber) Close() {
	p.once.Do(func() { close(p.stop) })
	p.wg.Wait()
}
