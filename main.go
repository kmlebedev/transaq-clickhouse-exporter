package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	log "github.com/sirupsen/logrus"
)

var connect driver.Conn

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
		log.Infof("Connect to ClickHouse %s (attempt %d/10)", clickhouseTarget(clickhouseOptions), attempt)
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

func main() {
	runCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	connect, err = openClickHouse(runCtx)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = connect.Close() }()

	provider, err := NewProvider(providerNameFromEnv())
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("TRANSAQ backend: %s", provider.Name())

	if err := runExporter(runCtx, provider, exportConfigFromEnv(), defaultReconnectConfig()); err != nil {
		log.Fatal(err)
	}
}

// clickhouseTarget описывает подключение для лога без пароля: CLICKHOUSE_URL
// несёт учётные данные, и печатать его целиком в journal нельзя.
func clickhouseTarget(options *clickhouse.Options) string {
	return fmt.Sprintf("%s/%s (user %s)", strings.Join(options.Addr, ","), options.Auth.Database, options.Auth.Username)
}
