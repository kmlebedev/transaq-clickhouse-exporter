package main

import (
	"context"
	"sync"

	log "github.com/sirupsen/logrus"
)

const eventQueueWarningSize = 1024

// eventQueue is an unbounded FIFO between a provider pump and Session.Run.
// Push never blocks: the legacy gRPC client sends into small typed channels
// and must keep draining them while ClickHouse or subscription recovery is
// slow.
type eventQueue struct {
	mu     sync.Mutex
	items  []Event
	closed bool
	wake   chan struct{}
}

func newEventQueue() *eventQueue {
	return &eventQueue{wake: make(chan struct{}, 1)}
}

func (q *eventQueue) Push(event Event) {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	q.items = append(q.items, event)
	q.mu.Unlock()
	q.signal()
}

// Close makes Pop return false once the queued events are drained.
func (q *eventQueue) Close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.signal()
}

func (q *eventQueue) signal() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// Pop returns the next event, or false when the queue is closed and empty.
// A cancelled context also returns false; the caller distinguishes the cases
// through its own ctx and terminal state.
func (q *eventQueue) Pop(ctx context.Context) (Event, bool) {
	for {
		q.mu.Lock()
		if len(q.items) > 0 {
			event := q.items[0]
			q.items[0] = nil
			q.items = q.items[1:]
			q.mu.Unlock()
			return event, true
		}
		if q.closed {
			q.mu.Unlock()
			return nil, false
		}
		q.mu.Unlock()
		select {
		case <-q.wake:
		case <-ctx.Done():
			return nil, false
		}
	}
}

// startQueuedWorker drains a provider channel into an unbounded queue and runs
// handle on every event. The client channels stay drained even when ClickHouse
// or subscription recovery is slow.
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

// startBufferedChannel keeps draining source while the consumer is slow. The
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
