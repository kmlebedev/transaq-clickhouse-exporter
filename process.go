package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	tcClient "github.com/kmlebedev/txmlconnector/client"
	"github.com/kmlebedev/txmlconnector/client/commands"
	log "github.com/sirupsen/logrus"
)

var errResponseStreamClosed = errors.New("txmlconnector response stream closed")

type transaqSessionConfig struct {
	restore       func(*tcClient.TCClient) error
	eventHandlers transaqEventHandlers
}

func defaultTransaqSessionConfig() transaqSessionConfig {
	return transaqSessionConfig{
		restore:       restoreSubscriptions,
		eventHandlers: defaultTransaqEventHandlers(),
	}
}

func runTransaq(
	runCtx context.Context,
	newClient tcClient.ClientFactory,
	sessionConfig transaqSessionConfig,
	reconnectConfig tcClient.ReconnectConfig,
) error {
	return tcClient.RunWithReconnect(
		runCtx,
		newClient,
		func(sessionCtx context.Context, client *tcClient.TCClient) error {
			return processTransaq(sessionCtx, client, sessionConfig)
		},
		reconnectConfig,
	)
}

func processTransaq(processCtx context.Context, client *tcClient.TCClient, config transaqSessionConfig) error {
	if config.restore == nil {
		return errors.New("TRANSAQ subscription restore callback is required")
	}
	eventWorkers := startTransaqEventWorkers(processCtx, client, config.eventHandlers)
	defer eventWorkers.stop()
	subscriptionsRestored := false
	for {
		select {
		case <-processCtx.Done():
			return processCtx.Err()
		case <-client.ShutdownChannel:
			return errResponseStreamClosed
		case status := <-eventWorkers.serverStatuses:
			switch status.Connected {
			case "true":
				if subscriptionsRestored {
					continue
				}
				if err := config.restore(client); err != nil {
					return fmt.Errorf("restore TRANSAQ subscriptions: %w", err)
				}
				subscriptionsRestored = true
				log.Info("TRANSAQ subscriptions restored")
			case "false", "error":
				return fmt.Errorf("TRANSAQ terminal is not connected: %+v", status)
			default:
				log.Infof("Status %+v", status)
			}
		case resp := <-client.ResponseChannel:
			switch resp {
			case "united_portfolio":
				log.Infof("UnitedPortfolio: ```\n%+v\n```", client.Data.UnitedPortfolio)
			case "united_equity":
				log.Infof("UnitedEquity: ```\n%+v\n```", client.Data.UnitedEquity)
			case "positions":
				// Todo avoid overwrite if only change field
				if client.Data.Positions.UnitedLimits != nil && len(client.Data.Positions.UnitedLimits) > 0 {
					positions.UnitedLimits = client.Data.Positions.UnitedLimits
				}
				if client.Data.Positions.SecPositions != nil && len(client.Data.Positions.SecPositions) > 0 {
					positions.SecPositions = client.Data.Positions.SecPositions
				}
				if client.Data.Positions.FortsMoney != nil && len(client.Data.Positions.FortsMoney) > 0 {
					positions.FortsMoney = client.Data.Positions.FortsMoney
				}
				if client.Data.Positions.MoneyPosition != nil && len(client.Data.Positions.MoneyPosition) > 0 {
					positions.MoneyPosition = client.Data.Positions.MoneyPosition
				}
				if client.Data.Positions.FortsPosition != nil && len(client.Data.Positions.FortsPosition) > 0 {
					positions.FortsPosition = client.Data.Positions.FortsPosition
				}
				if client.Data.Positions.FortsCollaterals != nil && len(client.Data.Positions.FortsCollaterals) > 0 {
					positions.FortsCollaterals = client.Data.Positions.FortsCollaterals
				}
				if client.Data.Positions.SpotLimit != nil && len(client.Data.Positions.SpotLimit) > 0 {
					positions.SpotLimit = client.Data.Positions.SpotLimit
				}
				if isAllTradesPositions {
					for _, secPosition := range client.Data.Positions.SecPositions {
						allTrades.Items = appendUniqueSecID(allTrades.Items, secPosition.SecId)
					}
				}
				log.Infof("Positions: \n%+v\n", client.Data.Positions)

			case "candles":
				batch, _ := connect.PrepareBatch(ctx, ChCandlesInsertQuery)
				dataCandleCountLock.Lock()
				dataCandleCount = len(client.Data.Candles.Items)
				dataCandleCountLock.Unlock()
				for _, candle := range client.Data.Candles.Items {
					candleDate, _ := time.Parse("02.01.2006 15:04:05", candle.Date)
					if err := batch.Append(
						fmt.Sprint(candleDate.Format("2006-01-02 15:04:05")),
						client.Data.Candles.SecCode,
						uint16(client.Data.Candles.Period),
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
				for _, quotation := range client.Data.Quotations.Items {
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
				log.Debugf("receive %s", resp)
			}
		}
	}
}

func restoreSubscriptions(client *tcClient.TCClient) error {
	// A disconnect leaves an incomplete minute in memory. Do not merge fresh
	// quotations into a candle that contains a gap in the source stream.
	clear(quotationCandles)
	if err := updateSecurities(client); err != nil {
		return err
	}
	if err := client.SendCommand(commands.Command{
		Id:         "subscribe",
		Quotations: quotations,
		AllTrades:  allTrades,
	}); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	log.Infof("Subscribe trades %+v", allTrades)

	for _, secID := range getSecuritiesInfo {
		if err := client.SendCommand(commands.Command{
			Id:    "get_securities_info",
			SecId: secID,
		}); err != nil {
			return fmt.Errorf("get securities info for %d: %w", secID, err)
		}
	}
	log.Infof("Get securities info %+v", getSecuritiesInfo)
	return nil
}

func appendUniqueSecID(items []int, secID int) []int {
	for _, item := range items {
		if item == secID {
			return items
		}
	}
	return append(items, secID)
}
