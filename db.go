package main

const (
	EnvKeyLogLevel          = "LOG_LEVEL"
	ExportCandleCount       = 0
	ChCandlesInsertQuery    = "INSERT INTO transaq_candles"
	ChSecuritiesInsertQuery = "INSERT INTO transaq_securities"
	ChTradesInsertQuery     = "INSERT INTO transaq_trades VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	ChSecInfoInsertQuery    = "INSERT INTO transaq_securities_info VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

	candlesDDL = `CREATE TABLE IF NOT EXISTS transaq_candles (
		date   DateTime('Europe/Moscow'),
		sec_code FixedString(16),
		period UInt8,
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
		lotsize UInt8,
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
		coupon_period UInt8,
		facevalue Float32,
		put_call FixedString(1),
		point_cost Float32,
		opt_type FixedString(1),
		lot_volume UInt8,
		isin String,
		regnumber String,
		buybackprice Float32,
		buybackdate Float32,
		currencyid String
	) ENGINE = ReplacingMergeTree()
	ORDER BY (sec_code, market, regnumber, isin)`
)
