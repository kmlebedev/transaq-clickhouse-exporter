package main

import (
	"context"
	"sync"

	tcClient "github.com/kmlebedev/txmlconnector/client"
	"github.com/kmlebedev/txmlconnector/client/commands"
	log "github.com/sirupsen/logrus"
)

const eventQueueWarningSize = 1024

type transaqEventHandlers struct {
	allTrades  func(context.Context, commands.AllTrades) error
	quotes     func(context.Context, commands.Quotes) error
	secInfo    func(context.Context, commands.SecInfo) error
	secInfoUpd func(context.Context, commands.SecInfoUpd) error
}

func defaultTransaqEventHandlers() transaqEventHandlers {
	return transaqEventHandlers{
		allTrades: insertTrades,
		quotes:    insertQuotes,
		secInfo:   insertSecInfo,
		secInfoUpd: func(_ context.Context, update commands.SecInfoUpd) error {
			log.Debugf("secInfoUpd %+v", update)
			return nil
		},
	}
}

type transaqEventWorkers struct {
	cancel         context.CancelFunc
	waitGroup      sync.WaitGroup
	serverStatuses <-chan commands.ServerStatus
}

func startTransaqEventWorkers(
	parent context.Context,
	client *tcClient.TCClient,
	handlers transaqEventHandlers,
) *transaqEventWorkers {
	workerCtx, cancel := context.WithCancel(parent)
	workers := &transaqEventWorkers{cancel: cancel}
	workers.serverStatuses = startBufferedChannel(
		workerCtx,
		&workers.waitGroup,
		"server status",
		client.ServerStatusChan,
	)
	startQueuedWorker(workerCtx, &workers.waitGroup, "all trades", client.AllTradesChan, handlers.allTrades)
	startQueuedWorker(workerCtx, &workers.waitGroup, "quotes", client.QuotesChan, handlers.quotes)
	startQueuedWorker(workerCtx, &workers.waitGroup, "security info", client.SecInfoChan, handlers.secInfo)
	startQueuedWorker(workerCtx, &workers.waitGroup, "security update", client.SecInfoUpdChan, handlers.secInfoUpd)
	return workers
}

func (workers *transaqEventWorkers) stop() {
	workers.cancel()
	workers.waitGroup.Wait()
}

func startQueuedWorker[T any](
	workerCtx context.Context,
	waitGroup *sync.WaitGroup,
	name string,
	source <-chan T,
	handle func(context.Context, T) error,
) {
	if source == nil {
		return
	}
	if handle == nil {
		handle = func(context.Context, T) error { return nil }
	}
	buffered := startBufferedChannel(workerCtx, waitGroup, name, source)
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		for {
			select {
			case <-workerCtx.Done():
				return
			case event, ok := <-buffered:
				if !ok {
					return
				}
				if err := handle(workerCtx, event); err != nil && workerCtx.Err() == nil {
					log.Errorf("Process TRANSAQ %s event: %v", name, err)
				}
			}
		}
	}()
}

// startBufferedChannel keeps draining the small channels exposed by
// txmlconnector even while ClickHouse or subscription recovery is slow. The
// queue deliberately preserves every financial event instead of dropping it.
func startBufferedChannel[T any](
	workerCtx context.Context,
	waitGroup *sync.WaitGroup,
	name string,
	source <-chan T,
) <-chan T {
	if source == nil {
		return nil
	}
	buffered := make(chan T)
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		defer close(buffered)

		queue := make([]T, 0)
		nextWarning := eventQueueWarningSize
		for {
			var output chan<- T
			var first T
			if len(queue) > 0 {
				output = buffered
				first = queue[0]
			}

			select {
			case <-workerCtx.Done():
				return
			case event, ok := <-source:
				if !ok {
					source = nil
					if len(queue) == 0 {
						return
					}
					continue
				}
				queue = append(queue, event)
				if len(queue) >= nextWarning {
					log.Warnf("TRANSAQ %s queue reached %d events", name, len(queue))
					nextWarning *= 2
				}
			case output <- first:
				var zero T
				queue[0] = zero
				queue = queue[1:]
			}
		}
	}()
	return buffered
}
