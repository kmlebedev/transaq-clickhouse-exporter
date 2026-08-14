package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	tcClient "github.com/kmlebedev/txmlconnector/client"
	log "github.com/sirupsen/logrus"
)

const reconnectIntervalEnv = "TC_RECONNECT_INTERVAL"

var errResponseStreamClosed = errors.New("txmlconnector response stream closed")

type reconnectConfig struct {
	terminalRetryInterval time.Duration
	sessionRetryMin       time.Duration
	sessionRetryMax       time.Duration
	sessionStableAfter    time.Duration
	disconnectTimeout     time.Duration
	restore               func(*tcClient.TCClient) error
	eventHandlers         transaqEventHandlers
}

func defaultReconnectConfig() reconnectConfig {
	terminalRetryInterval := 5 * time.Second
	if value := os.Getenv(reconnectIntervalEnv); value != "" {
		parsed, err := time.ParseDuration(value)
		if err != nil || parsed <= 0 {
			log.Warnf("Ignore invalid %s=%q; use %s", reconnectIntervalEnv, value, terminalRetryInterval)
		} else {
			terminalRetryInterval = parsed
		}
	}
	return reconnectConfig{
		terminalRetryInterval: terminalRetryInterval,
		sessionRetryMin:       time.Second,
		sessionRetryMax:       30 * time.Second,
		sessionStableAfter:    time.Minute,
		disconnectTimeout:     2 * time.Second,
		restore:               restoreSubscriptions,
		eventHandlers:         defaultTransaqEventHandlers(),
	}
}

func superviseTransaq(
	supervisorCtx context.Context,
	newClient func() (*tcClient.TCClient, error),
	config reconnectConfig,
) error {
	if newClient == nil {
		return errors.New("TRANSAQ client factory is required")
	}
	if config.restore == nil {
		return errors.New("TRANSAQ subscription restore callback is required")
	}
	if config.sessionRetryMin <= 0 {
		config.sessionRetryMin = time.Second
	}
	if config.sessionRetryMax < config.sessionRetryMin {
		config.sessionRetryMax = config.sessionRetryMin
	}
	if config.sessionStableAfter <= 0 {
		config.sessionStableAfter = time.Minute
	}

	retryDelay := config.sessionRetryMin
	for {
		if err := supervisorCtx.Err(); err != nil {
			return nil
		}

		client, err := newClient()
		if err != nil {
			if client != nil {
				client.Close()
			}
			log.Warnf("Create TRANSAQ client: %v; retry in %s", err, retryDelay)
			if err := waitForContext(supervisorCtx, retryDelay); err != nil {
				return nil
			}
			retryDelay = nextRetryDelay(retryDelay, config.sessionRetryMax)
			continue
		}
		if client == nil {
			log.Warnf("Create TRANSAQ client: factory returned nil; retry in %s", retryDelay)
			if err := waitForContext(supervisorCtx, retryDelay); err != nil {
				return nil
			}
			retryDelay = nextRetryDelay(retryDelay, config.sessionRetryMax)
			continue
		}

		sessionStarted := time.Now()
		err = processTransaq(supervisorCtx, client, config)
		closeTransaqClient(client, config.disconnectTimeout)
		if supervisorCtx.Err() != nil {
			return nil
		}
		if err == nil {
			return nil
		}
		if time.Since(sessionStarted) >= config.sessionStableAfter {
			retryDelay = config.sessionRetryMin
		}

		log.Warnf("TRANSAQ session stopped: %v; recreate client in %s", err, retryDelay)
		if err := waitForContext(supervisorCtx, retryDelay); err != nil {
			return nil
		}
		retryDelay = nextRetryDelay(retryDelay, config.sessionRetryMax)
	}
}

func closeTransaqClient(client *tcClient.TCClient, timeout time.Duration) {
	if client == nil {
		return
	}
	if timeout <= 0 {
		client.Close()
		return
	}

	disconnected := make(chan error, 1)
	go func() {
		disconnected <- client.Disconnect()
	}()
	select {
	case err := <-disconnected:
		if err != nil {
			log.Warnf("Disconnect TRANSAQ client: %v", err)
		}
	case <-time.After(timeout):
		log.Warnf("Disconnect TRANSAQ client timed out after %s", timeout)
	}
	client.Close()
}

func nextRetryDelay(current, maximum time.Duration) time.Duration {
	if current >= maximum/2 {
		return maximum
	}
	return current * 2
}

func waitForContext(waitCtx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-waitCtx.Done():
		return fmt.Errorf("wait interrupted: %w", waitCtx.Err())
	case <-timer.C:
		return nil
	}
}
