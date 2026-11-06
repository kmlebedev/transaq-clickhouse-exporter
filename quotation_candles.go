package main

import (
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

// quotationCandle is the in-memory minute bar built from <quotations> updates.
type quotationCandle struct {
	SecCode string
	Open    float64
	High    float64
	Low     float64
	Volume  int64
}

// quotationAggregator turns quotation updates into finished one-minute candles:
// the update at a :00 boundary closes the previous minute (the old exporter used
// the last traded price as the close). It keeps one bar per secid; a new session
// starts with an empty aggregator.
type quotationAggregator struct {
	candles map[int]*quotationCandle
}

func newQuotationAggregator() *quotationAggregator {
	return &quotationAggregator{candles: make(map[int]*quotationCandle)}
}

// Apply feeds one update and returns the candle closed by it, if any.
func (a *quotationAggregator) Apply(update QuotationEvent, now time.Time) []candleRow {
	state, exists := a.candles[update.SecID]
	if strings.HasSuffix(update.Time, ":00") && update.Last > 0 && exists {
		secCode := update.SecCode
		if secCode == "" {
			secCode = state.SecCode
		}
		candleTime, err := time.ParseInLocation(
			tableTimeLayout,
			now.In(moscowLocation).Format("2006-01-02")+" "+update.Time,
			moscowLocation,
		)
		if err != nil {
			log.Warnf("Quotation candle %s: parse time %q: %v", secCode, update.Time, err)
			return nil
		}
		row := candleRow{
			Time:    candleTime,
			SecCode: secCode,
			Period:  1,
			Open:    state.Open,
			Close:   update.Last,
			High:    state.High,
			Low:     state.Low,
			Volume:  clampUint64(state.Volume),
		}
		// The old exporter kept the secid in the map with a zero candle: the
		// next update starts a new bar through the normal accumulation branch.
		*state = quotationCandle{SecCode: state.SecCode}
		return []candleRow{row}
	}

	if exists {
		if state.Open == 0 && update.Open != 0 {
			state.Open = update.Open
		}
		if update.Last > state.High {
			state.High = update.Last
		}
		if update.Last < state.Low || state.Low == 0 {
			state.Low = update.Last
		}
		state.Volume += int64(update.Quantity)
		if update.SecCode != "" {
			state.SecCode = update.SecCode
		}
		return nil
	}

	a.candles[update.SecID] = &quotationCandle{
		SecCode: update.SecCode,
		Open:    update.Last,
		Low:     update.Last,
		High:    update.Last,
		Volume:  int64(update.Quantity),
	}
	return nil
}

func clampUint64(value int64) uint64 {
	if value < 0 {
		return 0
	}
	return uint64(value)
}
