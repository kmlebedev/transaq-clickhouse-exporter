package main

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/txmlconnector/client/commands"
)

type recordingConn struct {
	driver.Conn
	query string
	batch *recordingBatch
}

func (conn *recordingConn) PrepareBatch(
	_ context.Context,
	stringQuery string,
	_ ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	conn.query = stringQuery
	conn.batch = &recordingBatch{}
	return conn.batch, nil
}

type recordingBatch struct {
	driver.Batch
	rows [][]any
	sent bool
}

func (batch *recordingBatch) Append(values ...any) error {
	batch.rows = append(batch.rows, values)
	return nil
}

func (batch *recordingBatch) Send() error {
	batch.sent = true
	return nil
}

func (batch *recordingBatch) Close() error {
	return nil
}

func TestInsertTradesUsesOneBatchForWholeMessage(t *testing.T) {
	previousConnect := connect
	recorder := &recordingConn{}
	connect = recorder
	defer func() { connect = previousConnect }()

	err := insertTrades(context.Background(), commands.AllTrades{Items: []commands.Trade{
		{SecId: 1, TradeNo: 10, Time: "14.08.2026 12:00:00"},
		{SecId: 2, TradeNo: 20, Time: "14.08.2026 12:00:01"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if recorder.query != ChTradesInsertQuery {
		t.Fatalf("query = %q", recorder.query)
	}
	if len(recorder.batch.rows) != 2 {
		t.Fatalf("batch rows = %d, want 2", len(recorder.batch.rows))
	}
	if len(recorder.batch.rows[0]) != 10 {
		t.Fatalf("trade columns = %d, want 10", len(recorder.batch.rows[0]))
	}
	if !recorder.batch.sent {
		t.Fatal("trade batch was not sent")
	}
}

func TestInsertQuotesUsesOneBatchForWholeMessage(t *testing.T) {
	previousConnect := connect
	recorder := &recordingConn{}
	connect = recorder
	defer func() { connect = previousConnect }()

	err := insertQuotes(context.Background(), commands.Quotes{
		Time: time.Date(2026, time.August, 14, 12, 0, 0, 0, time.UTC),
		Items: []commands.Quote{
			{SecId: 1, Price: 100},
			{SecId: 2, Price: 200},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if recorder.query != ChQuotesInsert {
		t.Fatalf("query = %q", recorder.query)
	}
	if len(recorder.batch.rows) != 2 {
		t.Fatalf("batch rows = %d, want 2", len(recorder.batch.rows))
	}
	if len(recorder.batch.rows[0]) != 9 {
		t.Fatalf("quote columns = %d, want 9", len(recorder.batch.rows[0]))
	}
	if !recorder.batch.sent {
		t.Fatal("quote batch was not sent")
	}
}
