package main

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/kmlebedev/txmlconnector/client/commands"
	log "github.com/sirupsen/logrus"
)

const (
	EnvKeyLogLevel          = "LOG_LEVEL"
	ExportCandleCount       = 0
	tradeTimeLayout         = "02.01.2006 15:04:05"
	dateLayout              = "02.01.2006" // DD.MM.YYYY
	tableTimeLayout         = "2006-01-02 15:04:05"
	ChCandlesInsertQuery    = "INSERT INTO transaq_candles"
	ChSecuritiesInsertQuery = "INSERT INTO transaq_securities"
	ChTradesInsertQuery     = "INSERT INTO transaq_trades"
	ChSecInfoInsertQuery    = "INSERT INTO transaq_securities_info"
	ChQuotesInsert          = "INSERT INTO transaq_quotes"

	candlesDDL = `CREATE TABLE IF NOT EXISTS transaq_candles (
		date   DateTime('Europe/Moscow'),
		sec_code FixedString(16),
		period UInt16,
		open   Float32,
		close  Float32,
		high   Float32,
		low    Float32,
		volume UInt64
	) ENGINE = ReplacingMergeTree()
	ORDER BY (date, sec_code, period)`

	securitiesDDL = `CREATE TABLE IF NOT EXISTS transaq_securities (
		secid   UInt16,
		seccode FixedString(16),
		instrclass String,
		board String,
		market UInt8,
		shortname String,
		decimals UInt8,
		minstep Float32,
		lotsize UInt32,
        lotdivider UInt16,
		point_cost Float32,
		sectype String,
		quotestype UInt8
	) ENGINE = ReplacingMergeTree()
	ORDER BY (seccode, instrclass, board, market, sectype, quotestype)`

	tradesDDL = `CREATE TABLE IF NOT EXISTS transaq_trades (
		time   DateTime('Europe/Moscow'),
		secid   UInt16,
		sec_code LowCardinality(FixedString(16)),
        trade_no Int64,
		board LowCardinality(String),
		price   Float32,
		quantity UInt32,
        buy_sell LowCardinality(FixedString(1)),
        open_interest Int32,
        period LowCardinality(FixedString(1))
	) ENGINE = ReplacingMergeTree()
	ORDER BY (secid, board, sec_code, trade_no, time, buy_sell)`

	securitiesInfoDDL = `CREATE TABLE IF NOT EXISTS transaq_securities_info (
		secid   UInt16,
		sec_name String,
		sec_code FixedString(16),
		market UInt8,
		pname  String,
		mat_date DateTime('Europe/Moscow'),
		clearing_price Float32,
		minprice Float32,
		maxprice Float32,
		buy_deposit Float32,
		sell_deposit Float32,
		bgo_c Float32,
		bgo_nc Float32,
        bgo_buy Float32,
		accruedint Float32,
		coupon_value Float32,
		coupon_date DateTime('Europe/Moscow'),
		coupon_period UInt16,
		facevalue Float32,
		put_call FixedString(1),
		point_cost Float32,
		opt_type FixedString(1),
		lot_volume UInt32,
		isin String,
		regnumber String,
		buybackprice Float32,
		buybackdate DateTime('Europe/Moscow'),
		currencyid String
	) ENGINE = ReplacingMergeTree()
	ORDER BY (sec_code, market, regnumber, isin)`

	quotesDDL = `CREATE TABLE IF NOT EXISTS transaq_quotes (
        time 	 DateTime('Europe/Moscow'),
		secid    UInt16,
		board 	 LowCardinality(String),
		sec_code LowCardinality(FixedString(16)),
    	price    Float32,
		source   LowCardinality(String),
        yield    Int8,
        buy      Int16,
        Sell     Int16,
    ) ENGINE = ReplacingMergeTree()
	ORDER BY (sec_code, board, price, source)
    `
)

// Биржевое время Transaq: метки сделок приходят в московском времени без зоны.
var moscowLocation = time.FixedZone("MSK", 3*60*60)

// tradeRow is the transport-neutral row of transaq_trades.
type tradeRow struct {
	Time         time.Time
	SecID        int
	SecCode      string
	TradeNo      int64
	Board        string
	Price        float64
	Quantity     uint32
	BuySell      string
	OpenInterest int32
	Period       string
}

// quoteRow is the transport-neutral row of transaq_quotes.
type quoteRow struct {
	Time    time.Time
	SecID   int
	Board   string
	SecCode string
	Price   float64
	Source  string
	Yield   int8
	Buy     int16
	Sell    int16
}

// candleRow is the transport-neutral row of transaq_candles.
type candleRow struct {
	Time    time.Time
	SecCode string
	Period  uint16
	Open    float64
	Close   float64
	High    float64
	Low     float64
	Volume  uint64
}

func insertSecurities(insertCtx context.Context, items []Security) error {
	if len(items) == 0 {
		return nil
	}
	batchSec, err := connect.PrepareBatch(insertCtx, ChSecuritiesInsertQuery)
	if err != nil {
		return fmt.Errorf("prepare securities batch: %w", err)
	}
	defer batchSec.Close()
	for _, sec := range items {
		if err := batchSec.Append(uint16(sec.SecID),
			sec.SecCode,
			sec.InstrClass,
			sec.Board,
			uint8(sec.Market),
			sec.ShortName,
			uint8(sec.Decimals),
			float32(sec.MinStep),
			uint32(sec.LotSize),
			uint16(sec.LotDivider),
			float32(sec.PointCost),
			sec.SecType,
			uint8(sec.QuotesType)); err != nil {
			log.Error(err)
		}
	}
	if batchSec.Rows() > 0 {
		if err := batchSec.Send(); err != nil {
			return fmt.Errorf("send securities batch: %w", err)
		}
	}
	return nil
}

func insertTradeRows(insertCtx context.Context, rows []tradeRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := connect.PrepareBatch(insertCtx, ChTradesInsertQuery)
	if err != nil {
		return fmt.Errorf("prepare trades batch: %w", err)
	}
	defer batch.Close()
	for _, row := range rows {
		if err := batch.Append(
			row.Time,
			uint16(row.SecID),
			row.SecCode,
			row.TradeNo,
			row.Board,
			row.Price,
			row.Quantity,
			row.BuySell,
			row.OpenInterest,
			row.Period,
		); err != nil {
			return fmt.Errorf("append trade %d: %w", row.TradeNo, err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send trades batch: %w", err)
	}
	return nil
}

// insertTrades is the legacy gRPC adapter: one <alltrades> callback is one
// batch.
func insertTrades(insertCtx context.Context, trades commands.AllTrades) error {
	rows := make([]tradeRow, 0, len(trades.Items))
	for _, trade := range trades.Items {
		// Метка Transaq приходит биржевым временем без зоны. Строку в DateTime
		// clickhouse-go разбирает в часовом поясе процесса (time.Local), поэтому на
		// хосте с TZ America/New_York сделки уезжали на +7 часов; передаём момент.
		tradeTime, err := time.ParseInLocation(tradeTimeLayout, trade.Time, moscowLocation)
		if err != nil {
			return fmt.Errorf("trade %d: parse time %q: %w", trade.TradeNo, trade.Time, err)
		}
		log.Debugf("trade %s/%s raw=%q parsed=%s", trade.SecCode, trade.Board, trade.Time, tradeTime.Format(time.RFC3339))
		rows = append(rows, tradeRow{
			Time:         tradeTime,
			SecID:        trade.SecId,
			SecCode:      trade.SecCode,
			TradeNo:      trade.TradeNo,
			Board:        trade.Board,
			Price:        trade.Price,
			Quantity:     clampUint32(int64(trade.Quantity)),
			BuySell:      trade.BuySell,
			OpenInterest: clampInt32(int64(trade.OpenInterest)),
			Period:       trade.Period,
		})
	}
	return insertTradeRows(insertCtx, rows)
}

func insertQuoteRows(insertCtx context.Context, rows []quoteRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := connect.PrepareBatch(insertCtx, ChQuotesInsert)
	if err != nil {
		return fmt.Errorf("prepare quotes batch: %w", err)
	}
	defer batch.Close()
	for _, row := range rows {
		if err := batch.Append(
			row.Time,
			uint16(row.SecID),
			row.Board,
			row.SecCode,
			row.Price,
			row.Source,
			row.Yield,
			row.Buy,
			row.Sell,
		); err != nil {
			return fmt.Errorf("append quote %d: %w", row.SecID, err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send quotes batch: %w", err)
	}
	return nil
}

// insertQuotes is the legacy gRPC adapter.
func insertQuotes(insertCtx context.Context, quotes commands.Quotes) error {
	rows := make([]quoteRow, 0, len(quotes.Items))
	for _, quote := range quotes.Items {
		rows = append(rows, quoteRow{
			Time:    quotes.Time,
			SecID:   quote.SecId,
			Board:   quote.Board,
			SecCode: quote.SecCode,
			Price:   quote.Price,
			Source:  quote.Source,
			Yield:   clampInt8(int64(quote.Yield)),
			Buy:     clampInt16(int64(quote.Buy)),
			Sell:    clampInt16(int64(quote.Sell)),
		})
	}
	return insertQuoteRows(insertCtx, rows)
}

func insertCandleRows(insertCtx context.Context, rows []candleRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := connect.PrepareBatch(insertCtx, ChCandlesInsertQuery)
	if err != nil {
		return fmt.Errorf("prepare candles batch: %w", err)
	}
	defer batch.Close()
	for _, row := range rows {
		if err := batch.Append(
			row.Time,
			row.SecCode,
			row.Period,
			float32(row.Open),
			float32(row.Close),
			float32(row.High),
			float32(row.Low),
			row.Volume,
		); err != nil {
			return fmt.Errorf("append candle %s: %w", row.SecCode, err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send candles batch: %w", err)
	}
	return nil
}

// insertSecInfoRows writes the reference rows in one batch. The old exporter
// used AsyncInsert per callback; a batch is both faster and observable in
// system.query_log, while the table schema stays the same.
func insertSecInfoRows(insertCtx context.Context, infos []SecInfo) error {
	if len(infos) == 0 {
		return nil
	}
	batch, err := connect.PrepareBatch(insertCtx, ChSecInfoInsertQuery)
	if err != nil {
		return fmt.Errorf("prepare sec_info batch: %w", err)
	}
	defer batch.Close()
	for _, info := range infos {
		if err := batch.Append(secInfoArgs(info)...); err != nil {
			return fmt.Errorf("append sec_info %d: %w", info.SecID, err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send sec_info batch: %w", err)
	}
	return nil
}

// insertSecInfo is the legacy single-row adapter kept for compatibility tests.
func insertSecInfo(insertCtx context.Context, info commands.SecInfo) error {
	return insertSecInfoRows(insertCtx, []SecInfo{secInfoFromCommands(info)})
}

func secInfoFromCommands(info commands.SecInfo) SecInfo {
	return SecInfo{
		SecID:         info.SecId,
		SecName:       info.SecName,
		SecCode:       info.SecCode,
		Market:        info.Market,
		PName:         info.PName,
		MatDate:       parseDatePtr(info.MatDate),
		ClearingPrice: float64(info.ClearingPrice),
		MinPrice:      float64(info.MinPrice),
		MaxPrice:      float64(info.MaxPrice),
		BuyDeposit:    float64(info.BuyDeposit),
		SellDeposit:   float64(info.SellDeposit),
		BgoC:          float64(info.BgoC),
		BgoNC:         float64(info.BgoNc),
		BgoBuy:        float64(info.BgoBuy),
		AccruedInt:    float64(info.AccruedInt),
		CouponValue:   float64(info.CouponValue),
		CouponDate:    parseDatePtr(info.CouponDate),
		CouponPeriod:  info.CouponPeriod,
		FaceValue:     float64(info.FaceValue),
		PutCall:       info.PutCall,
		PointCost:     float64(info.PointCost),
		OptType:       info.OptType,
		LotVolume:     info.LotVolume,
		ISIN:          info.Isin,
		RegNumber:     info.RegNumber,
		BuybackPrice:  float64(info.BuybackPrice),
		BuybackDate:   parseDatePtr(info.BuybackDate),
		CurrencyID:    info.CurrencyId,
	}
}

func parseDatePtr(value string) *time.Time {
	if value == "" {
		return nil
	}
	parsed, err := time.Parse(dateLayout, value)
	if err != nil {
		return nil
	}
	return &parsed
}

func secInfoArgs(info SecInfo) []any {
	return []any{
		uint16(info.SecID),
		info.SecName,
		info.SecCode,
		uint8(info.Market),
		info.PName,
		secInfoDate(info.MatDate),
		info.ClearingPrice,
		info.MinPrice,
		info.MaxPrice,
		info.BuyDeposit,
		info.SellDeposit,
		info.BgoC,
		info.BgoNC,
		info.BgoBuy,
		info.AccruedInt,
		info.CouponValue,
		secInfoDate(info.CouponDate),
		uint16(info.CouponPeriod),
		info.FaceValue,
		info.PutCall,
		info.PointCost,
		info.OptType,
		uint32(info.LotVolume),
		info.ISIN,
		info.RegNumber,
		info.BuybackPrice,
		secInfoDate(info.BuybackDate),
		info.CurrencyID,
	}
}

// secInfoDate puts a date-only field at midnight MSK, matching the old string
// written into DateTime('Europe/Moscow'); a missing date becomes the epoch
// instead of an out-of-range year 1.
func secInfoDate(value *time.Time) time.Time {
	if value == nil {
		return time.Unix(0, 0)
	}
	return time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, moscowLocation)
}

func clampUint32(value int64) uint32 {
	switch {
	case value < 0:
		return 0
	case value > math.MaxUint32:
		return math.MaxUint32
	default:
		return uint32(value)
	}
}

func clampInt32(value int64) int32 {
	switch {
	case value < math.MinInt32:
		return math.MinInt32
	case value > math.MaxInt32:
		return math.MaxInt32
	default:
		return int32(value)
	}
}

func clampInt16(value int64) int16 {
	switch {
	case value < math.MinInt16:
		return math.MinInt16
	case value > math.MaxInt16:
		return math.MaxInt16
	default:
		return int16(value)
	}
}

func clampInt8(value int64) int8 {
	switch {
	case value < math.MinInt8:
		return math.MinInt8
	case value > math.MaxInt8:
		return math.MaxInt8
	default:
		return int8(value)
	}
}
