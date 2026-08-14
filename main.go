package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/txmlconnector/client"
	"github.com/kmlebedev/txmlconnector/client/commands"
	log "github.com/sirupsen/logrus"
)

var (
	ctx                  = context.Background()
	connect              driver.Conn
	quotations           = []commands.SubSecurity{}
	positions            = commands.Positions{}
	quotationCandles     = make(map[int]commands.Candle)
	dataCandleCount      = ExportCandleCount
	dataCandleCountLock  = sync.RWMutex{}
	isAllTradesPositions = false
	allTrades            = commands.SubAllTrades{}
	getSecuritiesInfo    = []int{}
	exportSecInfoNames   = []string{}
)

func init() {
	if lvl, err := log.ParseLevel(os.Getenv(EnvKeyLogLevel)); err == nil {
		log.SetLevel(lvl)
	}
}

func openClickHouse(openCtx context.Context) (driver.Conn, error) {
	clickhouseUrl := "tcp://127.0.0.1:9000"
	if chUrl := os.Getenv("CLICKHOUSE_URL"); chUrl != "" {
		clickhouseUrl = chUrl
	}
	clickhouseOptions, err := clickhouse.ParseDSN(clickhouseUrl)
	if err != nil {
		return nil, fmt.Errorf("parse ClickHouse DSN: %w", err)
	}
	conn, err := clickhouse.Open(clickhouseOptions)
	if err != nil {
		return nil, fmt.Errorf("open ClickHouse: %w", err)
	}

	var pingErr error
	for attempt := 1; attempt <= 10; attempt++ {
		log.Infof("Connect to ClickHouse %s (attempt %d/10)", clickhouseUrl, attempt)
		if pingErr = conn.Ping(openCtx); pingErr != nil {
			if exception, ok := pingErr.(*clickhouse.Exception); ok {
				log.Infof("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
			}
			log.Warn(pingErr)
		} else {
			break
		}
		if attempt < 10 {
			if err := waitForClickHouseRetry(openCtx, 3*time.Second); err != nil {
				_ = conn.Close()
				return nil, err
			}
		}
	}
	if pingErr != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("connect to ClickHouse after 10 attempts: %w", pingErr)
	}

	for _, ddl := range []string{candlesDDL, securitiesDDL, securitiesInfoDDL, tradesDDL, quotesDDL} {
		if err := conn.Exec(openCtx, ddl); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("initialize ClickHouse schema: %w", err)
		}
	}
	return conn, nil
}

func waitForClickHouseRetry(waitCtx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-waitCtx.Done():
		return waitCtx.Err()
	case <-timer.C:
		return nil
	}
}

func updateSecurities(client *tcClient.TCClient) error {
	isAllTradesPositions = false
	quotations = quotations[:0]
	allTrades.Items = allTrades.Items[:0]
	getSecuritiesInfo = getSecuritiesInfo[:0]
	exportSecInfoNames = exportSecInfoNames[:0]

	exportAllTradesSec := []string{}
	if envAllTrades := os.Getenv("EXPORT_ALL_TRADES"); envAllTrades != "" {
		for _, sec := range strings.Split(envAllTrades, ",") {
			if sec == "positions" {
				isAllTradesPositions = true
				continue
			}
			exportAllTradesSec = append(exportAllTradesSec, sec)
		}
	}
	// Get History data for all sec
	exportCandleCount := ExportCandleCount
	if eCandleCount, err := strconv.Atoi(os.Getenv("EXPORT_CANDLE_COUNT")); err == nil && eCandleCount > -2 {
		exportCandleCount = eCandleCount
	}
	exportSecBoards := []string{"TQBR", "TQCB", "FUT"}
	if eSecBoards := os.Getenv("EXPORT_SEC_BOARDS"); eSecBoards != "" {
		exportSecBoards = strings.Split(eSecBoards, ",")
	}
	exportSecCodes := []string{}
	if eSecCodes := os.Getenv("EXPORT_SEC_CODES"); eSecCodes != "" {
		exportSecCodes = strings.Split(eSecCodes, ",")
	}
	if names := os.Getenv("EXPORT_SEC_INFO_NAMES"); names != "" {
		exportSecInfoNames = strings.Split(names, ",")
	}
	exportPeriodSeconds := []string{}
	if ePeriodSeconds := os.Getenv("EXPORT_PERIOD_SECONDS"); ePeriodSeconds != "" {
		exportPeriodSeconds = strings.Split(ePeriodSeconds, ",")
	}
	batchSec, err := connect.PrepareBatch(ctx, ChSecuritiesInsertQuery)
	if err != nil {
		return fmt.Errorf("prepare securities batch: %w", err)
	}
	// TODO update allTRades if get message
	// Feb 21 12:01:57 rock-5b transaq_clickhouse_exporter[3732508]: time="2025-02-21T12:01:57+05:00" level=info msg="secInfoUpd {XMLName:{Space: Local:sec_info_upd} SecId:30338 Market:4 SecCode:CR9BC5 MinPrice:0 MaxPrice:0 BuyDeposit:0 Sell
	for _, sec := range client.Data.Securities.Items {
		exportSecBoardFound := false
		if slices.Contains(exportSecBoards, sec.Board) {
			exportSecBoardFound = true
		}
		if exportSecBoardFound && slices.Contains(exportAllTradesSec, sec.SecCode) {
			allTrades.Items = append(allTrades.Items, sec.SecId)
			//allTrades.Items = append(allTrades.Items, SubSecurity{Board: sec.Board, SecCode: sec.SecCode})
		}
		if sec.SecType == "BOND" {
			for _, secInfoName := range exportSecInfoNames {
				if strings.HasSuffix(sec.ShortName, secInfoName) {
					getSecuritiesInfo = append(getSecuritiesInfo, sec.SecId)
				}
			}
		}
		if sec.SecId == 0 || sec.Active != "true" || len(sec.SecCode) > 16 {
			continue
		}
		log.Debugf("%+v", sec)

		if err := batchSec.Append(uint16(sec.SecId),
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
		if !exportSecBoardFound {
			continue
		}
		if len(exportSecCodes) == 0 {
			continue
		}
		exportSecCodeFound := false
		for _, exportSecCode := range exportSecCodes {
			if exportSecCode == sec.SecCode || strings.Contains(sec.SecCode, exportSecCode) || exportSecCode == sec.ShortName || exportSecCode == "ALL" {
				exportSecCodeFound = true
				break
			}
		}
		if !exportSecCodeFound {
			continue
		}
		quotations = append(quotations, commands.SubSecurity{SecId: sec.SecId})
		for _, kind := range client.Data.CandleKinds.Items {
			if len(exportPeriodSeconds) > 0 {
				exportPeriodSecondFound := false
				for _, exportPeriodSecond := range exportPeriodSeconds {
					if exportPeriodSecond == strconv.Itoa(kind.Period) {
						exportPeriodSecondFound = true
					}
				}
				if !exportPeriodSecondFound {
					continue
				}
			}
			if exportCandleCount == 0 {
				continue
			} else if exportCandleCount > 0 {
				log.Debugf("gethistorydata sec %s period %d name %s seconds %d", sec.SecCode, kind.ID, kind.Name, kind.Period)
				if err = client.SendCommand(commands.Command{
					Id:     "gethistorydata",
					Period: kind.ID,
					SecId:  sec.SecId,
					Count:  exportCandleCount,
					Reset:  "true",
				}); err != nil {
					log.Error(err)
				}
				// Export All Candles
			} else {
				for ExportCandleCount == dataCandleCount {
					log.Debugf("loop get history %d == %d", ExportCandleCount, dataCandleCount)
					if err = client.SendCommand(commands.Command{
						Id:     "gethistorydata",
						Period: kind.ID,
						SecId:  sec.SecId,
						Count:  ExportCandleCount,
						Reset:  "false",
					}); err != nil {
						log.Error(err)
					}
					time.Sleep(2 * time.Second)
				}
				log.Debugf("exit loop get history %d == %d", ExportCandleCount, dataCandleCount)
				dataCandleCountLock.Lock()
				dataCandleCount = ExportCandleCount
				dataCandleCountLock.Unlock()
			}
		}
	}
	if batchSec.Rows() > 0 {
		if err := batchSec.Send(); err != nil {
			return fmt.Errorf("send securities batch: %w", err)
		}
	}
	return nil
}

func main() {
	runCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx = runCtx

	var err error
	connect, err = openClickHouse(runCtx)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = connect.Close() }()

	if err := runTransaq(
		runCtx,
		tcClient.NewTCClient,
		defaultTransaqSessionConfig(),
		tcClient.DefaultReconnectConfig(),
	); err != nil {
		log.Fatal(err)
	}
}
