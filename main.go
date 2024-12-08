package main

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/txmlconnector/client"
	"github.com/kmlebedev/txmlconnector/client/commands"
	log "github.com/sirupsen/logrus"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	ctx                  = context.Background()
	lvl                  log.Level
	tc                   *tcClient.TCClient
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
	var err error

	if lvl, err = log.ParseLevel(os.Getenv(EnvKeyLogLevel)); err == nil {
		log.SetLevel(lvl)
	}
	clickhouseUrl := "tcp://127.0.0.1:9000"
	if chUrl := os.Getenv("CLICKHOUSE_URL"); chUrl != "" {
		clickhouseUrl = chUrl
	}
	clickhouseOptions, _ := clickhouse.ParseDSN(clickhouseUrl)
	for i := 0; i < 10; i++ {
		log.Infof("Try connect to clickhouse %s", clickhouseUrl)
		if connect, err = clickhouse.Open(clickhouseOptions); err != nil {
			log.Fatal(err)
		}
		if err := connect.Ping(ctx); err != nil {
			if exception, ok := err.(*clickhouse.Exception); ok {
				log.Infof("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
			}
			log.Warn(err)
		} else {
			break
		}
		time.Sleep(3 * time.Second)
	}
	for _, ddl := range []string{candlesDDL, securitiesDDL, securitiesInfoDDL, tradesDDL, quotesDDL} {
		if err = connect.Exec(ctx, ddl); err != nil {
			log.Fatal(err)
		}
	}
}

func main() {
	defer func() {
		_ = tc.Disconnect()
		tc.Close()
		_ = connect.Close()
	}()

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

	go processTransaq()

	log.Infof("Wait txmlconnector ")
	for {
		if tc.Data.ServerStatus.Connected == "true" {
			log.Infof(" connected\n")
			break
		}
		fmt.Printf(".")
		time.Sleep(5 * time.Second)
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
		log.Error(err)
	}

	for _, sec := range tc.Data.Securities.Items {
		exportSecBoardFound := false
		if slices.Contains(exportSecBoards, sec.Board) {
			exportSecBoardFound = true
		}
		if exportSecBoardFound && slices.Contains(exportAllTradesSec, sec.SecCode) {
			allTrades.Items = append(allTrades.Items, sec.SecId)
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
			uint8(sec.LotSize),
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
		for _, kind := range tc.Data.CandleKinds.Items {
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
				log.Debugf(fmt.Sprintf("gethistorydata sec %s period %d name %s seconds %d", sec.SecCode, kind.ID, kind.Name, kind.Period))
				if err = tc.SendCommand(commands.Command{
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
					if err = tc.SendCommand(commands.Command{
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
			log.Error(err)
		}
	}
	<-tc.ShutdownChannel
}
