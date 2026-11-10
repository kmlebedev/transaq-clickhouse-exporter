package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// EnvTransaqBackend selects the market-data provider: native (default), grpc
// (the legacy txmlconnector-server client) or a future nats consumer.
const EnvTransaqBackend = "TRANSAQ_BACKEND"

const (
	backendNative = "native"
	backendGRPC   = "grpc"
)

// Provider opens a source of TRANSAQ market data. The exporter core knows the
// transport only through this interface, so a message-queue source (NATS) can
// be added without touching the export plan or the ClickHouse writers.
type Provider interface {
	// Name is the TRANSAQ_BACKEND value the provider was registered under.
	Name() string
	// Open establishes a session and returns only when the source is ready to
	// deliver events (catalog loaded, terminal connected, queue subscribed).
	// A returned error means "retry later"; the supervisor backs off.
	Open(ctx context.Context) (Session, error)
}

// Session is one live market-data session. Run delivers normalized events
// until ctx is cancelled (then it returns nil) or the source fails (then it
// returns an error and the supervisor reopens the provider).
type Session interface {
	Run(ctx context.Context, h func(Event) error) error
	Close() error
}

// CatalogSource is implemented by sessions that expose the instrument catalog.
// A pure queue consumer may omit it: the core then only writes the incoming
// stream (no securities table, no subscription, no history).
type CatalogSource interface {
	Catalog() Catalog
}

// Subscriber is implemented by sessions that can select the market feeds.
type Subscriber interface {
	Subscribe(ctx context.Context, sub Subscription) error
}

// SecuritiesInfoSource exposes the reference data of the selected securities.
// failed counts definitive per-security failures (a rejected command or a
// missing spec); a timeout is not definitive and is reported only in the log.
type SecuritiesInfoSource interface {
	SecuritiesInfo(ctx context.Context, symbols []Symbol) (infos []SecInfo, failed int, err error)
}

// HistorySource serves gethistorydata series.
type HistorySource interface {
	History(ctx context.Context, req HistoryRequest) ([]Candle, error)
}

// PositionSource returns the positions known at session start (the `positions`
// export mode subscribes their feeds immediately).
type PositionSource interface {
	Positions() []Symbol
}

// Catalog is the per-session instrument snapshot.
type Catalog struct {
	Securities  []Security
	CandleKinds []CandleKind
}

// Security is the transport-neutral form of a <security> record.
type Security struct {
	SecID      int
	Active     bool
	SecCode    string
	InstrClass string
	Board      string
	Market     int
	ShortName  string
	Decimals   int
	MinStep    float64
	LotSize    int
	LotDivider int
	PointCost  float64
	SecType    string
	QuotesType int
}

// Symbol identifies a security on a board: the stable key across sessions.
type Symbol struct {
	Board   string
	SecCode string
}

func (s Symbol) String() string { return s.Board + ":" + s.SecCode }

// CandleKind is one <candlekinds> entry: ID is the gethistorydata period.
type CandleKind struct {
	ID     int
	Period int
	Name   string
}

// Subscription is the feed selection of one session.
type Subscription struct {
	AllTrades  []Symbol
	Quotations []Symbol
}

// HistoryRequest is one gethistorydata query.
type HistoryRequest struct {
	Symbol Symbol
	Period int
	Count  int
	Reset  bool
}

// SecInfo is the transport-neutral <sec_info> row. Missing fields are zero.
type SecInfo struct {
	SecID         int
	SecName       string
	SecCode       string
	Market        int
	PName         string
	MatDate       *time.Time
	ClearingPrice float64
	MinPrice      float64
	MaxPrice      float64
	BuyDeposit    float64
	SellDeposit   float64
	BgoC          float64
	BgoNC         float64
	BgoBuy        float64
	// AccruedInt, CouponValue, FaceValue, PointCost, BuybackPrice are in the
	// price/currency units of the instrument.
	AccruedInt   float64
	CouponValue  float64
	CouponDate   *time.Time
	CouponPeriod int
	FaceValue    float64
	PutCall      string
	PointCost    float64
	OptType      string
	LotVolume    int
	ISIN         string
	RegNumber    string
	BuybackPrice float64
	BuybackDate  *time.Time
	CurrencyID   string
}

// Candle is one OHLCV bar. Time is the UTC instant.
type Candle struct {
	Time   time.Time
	Open   float64
	Close  float64
	High   float64
	Low    float64
	Volume int64
}

// QuoteLevel is one order-book level; Buy/Sell keep the -1 deletion marker.
type QuoteLevel struct {
	Price  float64
	Source string
	Yield  int
	Buy    int64
	Sell   int64
}

// Event is one normalized market-data event. The interface is closed: only
// this package implements it.
type Event interface{ isEvent() }

type TradeEvent struct {
	SecID        int
	Board        string
	SecCode      string
	TradeNo      int64
	Time         time.Time
	Price        float64
	Quantity     uint32
	BuySell      string
	OpenInterest int32
	Period       string
}

func (TradeEvent) isEvent() {}

type QuotesEvent struct {
	SecID   int
	Board   string
	SecCode string
	Time    time.Time
	Levels  []QuoteLevel
}

func (QuotesEvent) isEvent() {}

// QuotationEvent carries only the fields the minute-candle aggregator uses.
type QuotationEvent struct {
	SecID    int
	Board    string
	SecCode  string
	Time     string
	Open     float64
	Last     float64
	Quantity int
}

func (QuotationEvent) isEvent() {}

// CandlesEvent is a candle series that arrived without a matching History
// call (the legacy gRPC stream may deliver one).
type CandlesEvent struct {
	SecID   int
	Board   string
	SecCode string
	Period  int
	Items   []Candle
}

func (CandlesEvent) isEvent() {}

type PositionEvent struct {
	Symbol Symbol
}

func (PositionEvent) isEvent() {}

type SecInfoUpdateEvent struct {
	Symbol Symbol
}

func (SecInfoUpdateEvent) isEvent() {}

// StatusEvent reports a source state change (TRANSAQ server_status or the
// equivalent of another provider).
type StatusEvent struct {
	Connected bool
	Recover   bool
	Err       string
}

func (StatusEvent) isEvent() {}

// CatalogAddedEvent carries securities the source added to its catalog after
// the session opened. After a night restart the broker sends the catalog in
// sections (2026-11-10: the session opened on FUT only, TQBR and TQOB came
// later), so the exporter subscribes to the late part of the universe.
type CatalogAddedEvent struct {
	Securities []Security
}

func (CatalogAddedEvent) isEvent() {}

// UnknownEvent is a source record the provider could not decode.
type UnknownEvent struct {
	Tag    byte
	Offset int
}

func (UnknownEvent) isEvent() {}

// ProviderFactory builds a provider from the environment, validating its
// configuration eagerly so a typo fails at startup instead of retrying forever.
type ProviderFactory func() (Provider, error)

var (
	providerMu    sync.RWMutex
	providerTable = map[string]ProviderFactory{}
)

// RegisterProvider adds a provider factory; providers register from init().
func RegisterProvider(name string, factory ProviderFactory) {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		panic("RegisterProvider: empty name")
	}
	if factory == nil {
		panic("RegisterProvider: nil factory for " + name)
	}
	providerMu.Lock()
	defer providerMu.Unlock()
	if _, exists := providerTable[name]; exists {
		panic("RegisterProvider: duplicate provider " + name)
	}
	providerTable[name] = factory
}

// NewProvider resolves TRANSAQ_BACKEND and builds the provider. An empty name
// selects the native provider.
func NewProvider(name string) (Provider, error) {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		name = backendNative
	}
	providerMu.RLock()
	factory, ok := providerTable[name]
	providerMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown TRANSAQ provider %q (available: %s)", name, strings.Join(providerNames(), ", "))
	}
	return factory()
}

func providerNameFromEnv() string { return os.Getenv(EnvTransaqBackend) }

func providerNames() []string {
	providerMu.RLock()
	names := make([]string, 0, len(providerTable))
	for name := range providerTable {
		names = append(names, name)
	}
	providerMu.RUnlock()
	sort.Strings(names)
	return names
}

// catalogIndex is the map form of Catalog for event hot paths.
type catalogIndex struct {
	byID     map[int]Security
	bySymbol map[Symbol]Security
}

func newCatalogIndex(c Catalog) catalogIndex {
	index := catalogIndex{
		byID:     make(map[int]Security, len(c.Securities)),
		bySymbol: make(map[Symbol]Security, len(c.Securities)),
	}
	for _, sec := range c.Securities {
		index.byID[sec.SecID] = sec
		index.bySymbol[sec.Symbol()] = sec
	}
	return index
}

func (i catalogIndex) symbolByID(secID int) (Symbol, bool) {
	sec, ok := i.byID[secID]
	if !ok {
		return Symbol{}, false
	}
	return sec.Symbol(), true
}

// Symbol is the stable key of the security on its default board.
func (s Security) Symbol() Symbol { return Symbol{Board: s.Board, SecCode: s.SecCode} }
