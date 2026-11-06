package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const historyWorkerCount = 4

// reconnectConfig controls the provider reopen backoff.
type reconnectConfig struct {
	RetryMin           time.Duration
	RetryMax           time.Duration
	SessionStableAfter time.Duration
}

// defaultReconnectConfig keeps the settings of the previous client supervisor:
// 1 s -> 30 s, reset after a stable minute.
func defaultReconnectConfig() reconnectConfig {
	return reconnectConfig{
		RetryMin:           time.Second,
		RetryMax:           30 * time.Second,
		SessionStableAfter: time.Minute,
	}
}

func (c reconnectConfig) normalized() reconnectConfig {
	defaults := defaultReconnectConfig()
	if c.RetryMin <= 0 {
		c.RetryMin = defaults.RetryMin
	}
	if c.RetryMax <= 0 {
		c.RetryMax = defaults.RetryMax
	}
	if c.RetryMax < c.RetryMin {
		c.RetryMax = c.RetryMin
	}
	if c.SessionStableAfter <= 0 {
		c.SessionStableAfter = defaults.SessionStableAfter
	}
	return c
}

// runExporter supervises the selected provider: it opens a session, runs the
// export session on it and reopens the provider with an exponential backoff
// after a failure. The retry delay resets after a stable session.
func runExporter(ctx context.Context, provider Provider, cfg exportConfig, reconnect reconnectConfig) error {
	reconnect = reconnect.normalized()
	delay := reconnect.RetryMin
	for {
		if ctx.Err() != nil {
			return nil
		}
		session, err := provider.Open(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Warnf("Open TRANSAQ %s session: %v; retry in %s", provider.Name(), err, delay)
			if waitForContext(ctx, delay) != nil {
				return nil
			}
			delay = nextRetryDelay(delay, reconnect.RetryMax)
			continue
		}

		started := time.Now()
		err = runExporterSession(ctx, session, cfg)
		if closeErr := session.Close(); closeErr != nil {
			log.Debugf("Close TRANSAQ %s session: %v", provider.Name(), closeErr)
		}
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			return nil
		}
		if time.Since(started) >= reconnect.SessionStableAfter {
			delay = reconnect.RetryMin
		}
		log.Warnf("TRANSAQ %s session stopped: %v; reopen in %s", provider.Name(), err, delay)
		if waitForContext(ctx, delay) != nil {
			return nil
		}
		delay = nextRetryDelay(delay, reconnect.RetryMax)
	}
}

// runExporterSession wires one provider session to the export plan and the
// ClickHouse writers. Optional provider capabilities are used when present, so a
// pure queue consumer (NATS) can omit the catalog, subscription and history.
func runExporterSession(ctx context.Context, session Session, cfg exportConfig) error {
	var (
		plan          exportPlan
		catalogSource CatalogSource
		hasCatalog    bool
		index         catalogIndex
		catalogSize   int
	)
	if source, ok := session.(CatalogSource); ok {
		catalog := source.Catalog()
		catalogSource = source
		hasCatalog = true
		catalogSize = len(catalog.Securities)
		index = newCatalogIndex(catalog)
		plan = buildExportPlan(catalog, cfg)
		if err := insertSecurities(ctx, plan.Securities); err != nil {
			return fmt.Errorf("insert securities: %w", err)
		}
	}

	subscriber, canSubscribe := session.(Subscriber)
	subscribed := false
	if hasCatalog && cfg.Positions {
		if positions, ok := session.(PositionSource); ok {
			for _, symbol := range positions.Positions() {
				sec, ok := index.bySymbol[symbol]
				if !ok {
					continue
				}
				plan.AllTrades = appendUniqueSecurity(plan.AllTrades, sec)
			}
		}
	}
	if hasCatalog && canSubscribe {
		if err := checkSubscriptions(catalogSize, len(plan.AllTrades), len(plan.Quotations)); err != nil {
			return err
		}
		if len(plan.AllTrades) > 0 || len(plan.Quotations) > 0 {
			if err := subscriber.Subscribe(ctx, plan.Subscription()); err != nil {
				return fmt.Errorf("subscribe: %w", err)
			}
			subscribed = true
			log.Infof("Subscribe trades %d, quotations %d", len(plan.AllTrades), len(plan.Quotations))
		}
	} else if hasCatalog {
		log.Debugf("provider %T does not support subscriptions; feeds are configured by the source", session)
	}

	if hasCatalog && len(plan.SecInfo) > 0 {
		if source, ok := session.(SecuritiesInfoSource); ok {
			symbols := plan.secInfoSymbols()
			infos, failed, err := source.SecuritiesInfo(ctx, symbols)
			if err != nil {
				return fmt.Errorf("get securities info: %w", err)
			}
			if err := insertSecInfoRows(ctx, infos); err != nil {
				return fmt.Errorf("insert sec info: %w", err)
			}
			log.Infof("Get securities info: requested %d, received %d, failed %d", len(symbols), len(infos), failed)
			if err := checkSecuritiesInfo(len(symbols), failed); err != nil {
				return err
			}
		} else {
			log.Debugf("provider %T cannot serve sec_info; %d symbols skipped", session, len(plan.SecInfo))
		}
	}
	// The deploy smoke test waits for this exact line. It is emitted after the
	// whole session setup (subscription and sec_info), not right after subscribe.
	if subscribed {
		log.Infof("TRANSAQ subscriptions restored")
	}

	dispatcher := newEventDispatcher(ctx, cfg, subscriber, plan.Subscription().AllTrades)
	defer dispatcher.Close()

	historyCtx, cancelHistory := context.WithCancel(ctx)
	var historyWG sync.WaitGroup
	if hasCatalog && len(plan.History) > 0 {
		if source, ok := session.(HistorySource); ok {
			historyWG.Add(1)
			go func() {
				defer historyWG.Done()
				runHistoryRequests(historyCtx, source, plan, dispatcher.WriteCandles)
			}()
		} else {
			log.Debugf("provider %T cannot serve history; %d requests skipped", session, len(plan.History))
		}
	}

	stopCatalogLog := startCatalogLogger(ctx, catalogSource, hasCatalog, plan)
	defer stopCatalogLog()
	runErr := session.Run(ctx, dispatcher.Handle)
	cancelHistory()
	historyWG.Wait()
	return runErr
}

func runHistoryRequests(ctx context.Context, source HistorySource, plan exportPlan, write func(secCode string, period int, items []Candle)) {
	const maxHistoryCount = 65535 // the native RQ_HISTORY count is uint16
	count, reset := plan.CandleCount, true
	if count < 0 {
		count, reset = 0, false
	}
	if count > maxHistoryCount {
		log.Warnf("EXPORT_CANDLE_COUNT=%d exceeds %d; clamped", count, maxHistoryCount)
		count = maxHistoryCount
	}
	queue := make(chan historyRequest)
	var workers sync.WaitGroup
	for i := 0; i < historyWorkerCount; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for request := range queue {
				candles, err := source.History(ctx, HistoryRequest{
					Symbol: request.Security.Symbol(),
					Period: request.Kind.ID,
					Count:  count,
					Reset:  reset,
				})
				if err != nil {
					if ctx.Err() == nil {
						log.Warnf("gethistorydata %s period %d: %v", request.Security.Symbol(), request.Kind.ID, err)
					}
					continue
				}
				if len(candles) == 0 {
					continue
				}
				log.Infof("gethistorydata %s period %d: %d candles", request.Security.Symbol(), request.Kind.ID, len(candles))
				write(request.Security.SecCode, request.Kind.ID, candles)
			}
		}()
	}
	for _, request := range plan.History {
		select {
		case queue <- request:
		case <-ctx.Done():
			close(queue)
			workers.Wait()
			return
		}
	}
	close(queue)
	workers.Wait()
}

// startCatalogLogger logs the catalog summary at session start and then once
// per interval while the session is running. The returned function stops the
// goroutine and waits for it, so a test may safely restore catalogLogEvery.
func startCatalogLogger(ctx context.Context, source CatalogSource, hasCatalog bool, plan exportPlan) func() {
	if !hasCatalog || source == nil {
		return func() {}
	}
	logger := &catalogLogger{}
	subscribed := plan.subscribedSecIDs()
	items := source.Catalog().Securities
	logger.log(items, subscribed, time.Now())

	interval := catalogLogEvery
	stop := make(chan struct{})
	done := make(chan struct{})
	var once sync.Once
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case now := <-ticker.C:
				logger.log(source.Catalog().Securities, subscribed, now)
			}
		}
	}()
	return func() {
		once.Do(func() { close(stop) })
		<-done
	}
}

func appendUniqueSecurity(items []Security, sec Security) []Security {
	for _, item := range items {
		if item.SecID == sec.SecID {
			return items
		}
	}
	return append(items, sec)
}

// checkSubscriptions не даёт подписаться на пустой список: в окне ночного
// рестарта брокер может отдать промежуточный каталог без нужных бумаг. Тогда
// подписка пуста, сервер отвечает «Неверный формат команды», а сессия живёт
// без сделок до ручного рестарта. Ошибка пересоздаёт сессию.
func checkSubscriptions(catalog, trades, quotes int) error {
	if trades == 0 && quotes == 0 {
		return fmt.Errorf("TRANSAQ catalog is not ready: none of %d securities match the export config", catalog)
	}
	return nil
}

// checkSecuritiesInfo отличает единичные отказы (погашенная бумага в каталоге)
// от неготового брокера: в окне ночного рестарта сервер отдаёт промежуточный
// каталог, отказывает по большинству sec_info и молча игнорирует подписку на
// сделки. Тогда сессия пересоздаётся и подписка повторяется.
func checkSecuritiesInfo(requested, failed int) error {
	if requested > 0 && failed*2 > requested {
		return fmt.Errorf("TRANSAQ catalog is not ready: securities info failed for %d of %d", failed, requested)
	}
	return nil
}

func waitForContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func nextRetryDelay(current, maximum time.Duration) time.Duration {
	if current >= maximum/2 {
		return maximum
	}
	return current * 2
}
