package main

import (
	"fmt"
	"github.com/kmlebedev/txmlconnector/client/commands"
	log "github.com/sirupsen/logrus"
	"strings"
	"time"
)

func processTransaq() {
	var status commands.ServerStatus
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case upd := <-tc.SecInfoUpdChan:
			log.Infof("secInfoUpd %+v", upd)
		case status = <-tc.ServerStatusChan:
			switch status.Connected {
			case "true":
				log.Infof("server status is true")
				if err := tc.SendCommand(commands.Command{
					Id:         "subscribe",
					Quotations: quotations,
					AllTrades:  allTrades,
				}); err != nil {
					log.Error("SendCommand subscribe: ", err)
				}
				for _, secId := range getSecuritiesInfo {
					if err := tc.SendCommand(commands.Command{
						Id:    "get_securities_info",
						SecId: secId,
					}); err != nil {
						log.Error("SendCommand get_securities_info: ", err)
					}
				}
			case "error":
				log.Warnf("txmlconnector not connected %+v\n", status)
			default:
				log.Infof("Status %+v", status)
			}
		case <-ticker.C:
			if status.Connected == "true" {
				continue
			}
			if err := tc.Connect(); err != nil {
				log.Error("reconnect", err)
			}
		case trades := <-tc.AllTradesChan:
			for _, trade := range trades.Items {
				if err := insertTrade(&trade); err != nil {
					log.Errorf("trades async insert trade: %+v: %+v", trade, err)
				}
			}
		case quotes := <-tc.QuotesChan:
			{
				for _, quote := range quotes.Items {
					if err := insertQuote(&quote, &quotes.Time); err != nil {
						log.Errorf("trades async insert quote: %+v: %+v", quote, err)
					}
				}
			}
		case secInfo := <-tc.SecInfoChan:
			if err := insertSecInfo(&secInfo); err != nil {
				log.Errorf("trades async insert secInfo: %+v: %+v", secInfo, err)
			}
		case resp := <-tc.ResponseChannel:
			switch resp {
			case "united_portfolio":
				log.Infof(fmt.Sprintf("UnitedPortfolio: ```\n%+v\n```", tc.Data.UnitedPortfolio))
			case "united_equity":
				log.Infof(fmt.Sprintf("UnitedEquity: ```\n%+v\n```", tc.Data.UnitedEquity))
			case "positions":
				// Todo avoid overwrite if only change field
				if tc.Data.Positions.UnitedLimits != nil && len(tc.Data.Positions.UnitedLimits) > 0 {
					positions.UnitedLimits = tc.Data.Positions.UnitedLimits
				}
				if tc.Data.Positions.SecPositions != nil && len(tc.Data.Positions.SecPositions) > 0 {
					positions.SecPositions = tc.Data.Positions.SecPositions
				}
				if tc.Data.Positions.FortsMoney != nil && len(tc.Data.Positions.FortsMoney) > 0 {
					positions.FortsMoney = tc.Data.Positions.FortsMoney
				}
				if tc.Data.Positions.MoneyPosition != nil && len(tc.Data.Positions.MoneyPosition) > 0 {
					positions.MoneyPosition = tc.Data.Positions.MoneyPosition
				}
				if tc.Data.Positions.FortsPosition != nil && len(tc.Data.Positions.FortsPosition) > 0 {
					positions.FortsPosition = tc.Data.Positions.FortsPosition
				}
				if tc.Data.Positions.FortsCollaterals != nil && len(tc.Data.Positions.FortsCollaterals) > 0 {
					positions.FortsCollaterals = tc.Data.Positions.FortsCollaterals
				}
				if tc.Data.Positions.SpotLimit != nil && len(tc.Data.Positions.SpotLimit) > 0 {
					positions.SpotLimit = tc.Data.Positions.SpotLimit
				}
				if isAllTradesPositions {
					for _, secPosition := range tc.Data.Positions.SecPositions {
						allTrades.Items = append(allTrades.Items, secPosition.SecId)
					}
				}
				log.Infof("Positions: \n%+v\n", tc.Data.Positions)
			case "candles":
				batch, _ := connect.PrepareBatch(ctx, ChCandlesInsertQuery)
				dataCandleCountLock.Lock()
				dataCandleCount = len(tc.Data.Candles.Items)
				dataCandleCountLock.Unlock()
				for _, candle := range tc.Data.Candles.Items {
					candleDate, _ := time.Parse("02.01.2006 15:04:05", candle.Date)
					if err := batch.Append(
						fmt.Sprint(candleDate.Format("2006-01-02 15:04:05")),
						tc.Data.Candles.SecCode,
						uint8(tc.Data.Candles.Period),
						float32(candle.Open),
						float32(candle.Close),
						float32(candle.High),
						float32(candle.Low),
						uint64(candle.Volume),
					); err != nil {
						log.Error(err)
					}
				}
				if err := batch.Send(); err != nil {
					log.Error(err)
				}
			case "quotations":
				timeNow := time.Now()
				batch, _ := connect.PrepareBatch(ctx, ChCandlesInsertQuery)
				for _, quotation := range tc.Data.Quotations.Items {
					quotationCandle, quotationCandleExist := quotationCandles[quotation.SecId]
					if strings.HasSuffix(quotation.Time, ":00") && quotation.Last > 0 && quotationCandleExist {
						if err := batch.Append(
							fmt.Sprintf("%s %s", timeNow.Format("2006-01-02"), quotation.Time),
							quotation.SecCode,
							uint8(1),
							float32(quotationCandles[quotation.SecId].Open),
							float32(quotation.Last), // Close
							float32(quotationCandles[quotation.SecId].High),
							float32(quotationCandles[quotation.SecId].Low),
							uint64(quotationCandles[quotation.SecId].Volume),
						); err != nil {
							log.Fatal(err)
						}
						quotationCandles[quotation.SecId] = commands.Candle{}
					} else {
						if quotationCandleExist {
							if quotationCandle.Open == 0 && quotation.Open != 0 {
								quotationCandle.Open = quotation.Open
							}
							if quotation.Last > quotationCandle.High {
								quotationCandle.High = quotation.Last
							}
							if quotation.Last < quotationCandle.Low || quotationCandle.Low == 0 {
								quotationCandle.Low = quotation.Last
							}
							quotationCandle.Volume += int64(quotation.Quantity)
						} else {
							quotationCandles[quotation.SecId] = commands.Candle{
								Open:   quotation.Last,
								Low:    quotation.Last,
								High:   quotation.Last,
								Volume: int64(quotation.Quantity),
							}
						}
					}
				}
				if err := batch.Send(); err != nil {
					log.Error(err)
				}
			default:
				log.Debugf(fmt.Sprintf("receive %s", resp))
			}
		}
	}
}
