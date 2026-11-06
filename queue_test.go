package main

import (
	"context"
	"sync"
	"testing"
	"time"
)

// startBufferedChannel must keep draining its source while the consumer is
// busy: a slow ClickHouse insert must not block the provider poll loop.
func TestBufferedChannelKeepsDrainingSlowConsumer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var pump sync.WaitGroup
	source := make(chan int)
	buffered := startBufferedChannel(ctx, &pump, "test", source)

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		<-buffered
		time.Sleep(100 * time.Millisecond)
		for range buffered {
		}
	}()

	sent := make(chan struct{})
	go func() {
		defer close(sent)
		for i := 0; i < 10000; i++ {
			select {
			case source <- i:
			case <-ctx.Done():
				return
			}
		}
	}()
	select {
	case <-sent:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("источник заблокирован медленным потребителем")
	}

	cancel()
	pump.Wait()
	<-consumerDone
}
