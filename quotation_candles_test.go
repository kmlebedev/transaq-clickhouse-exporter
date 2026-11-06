package main

import (
	"testing"
	"time"
)

func TestQuotationAggregatorClosesMinute(t *testing.T) {
	aggregator := newQuotationAggregator()
	now := time.Date(2026, time.August, 14, 10, 1, 2, 0, moscowLocation)

	if rows := aggregator.Apply(QuotationEvent{
		SecID: 1, Board: "TQBR", SecCode: "SBER", Time: "10:00:30", Open: 99, Last: 100, Quantity: 5,
	}, now); len(rows) != 0 {
		t.Fatalf("первая сделка не должна закрывать минуту: %+v", rows)
	}
	if rows := aggregator.Apply(QuotationEvent{
		SecID: 1, Board: "TQBR", SecCode: "SBER", Time: "10:00:45", Last: 101, Quantity: 2,
	}, now); len(rows) != 0 {
		t.Fatalf("внутриминутное обновление не должно закрывать минуту: %+v", rows)
	}
	rows := aggregator.Apply(QuotationEvent{
		SecID: 1, Board: "TQBR", SecCode: "SBER", Time: "10:01:00", Last: 102,
	}, now)
	if len(rows) != 1 {
		t.Fatalf("минута не закрыта: %+v", rows)
	}
	row := rows[0]
	// The legacy exporter stamps the closed bar with the boundary update time
	// (10:01:00), not with the previous minute.
	wantTime := time.Date(2026, time.August, 14, 10, 1, 0, 0, moscowLocation)
	if !row.Time.Equal(wantTime) {
		t.Fatalf("time = %s, want %s", row.Time, wantTime)
	}
	if row.SecCode != "SBER" || row.Period != 1 {
		t.Fatalf("row = %+v", row)
	}
	if row.Open != 100 || row.Close != 102 || row.High != 101 || row.Low != 100 || row.Volume != 7 {
		t.Fatalf("OHLCV = %+v", row)
	}

	// После закрытия минуты начинается новая свеча.
	if rows := aggregator.Apply(QuotationEvent{
		SecID: 1, Board: "TQBR", SecCode: "SBER", Time: "10:01:15", Open: 102, Last: 103, Quantity: 1,
	}, now); len(rows) != 0 {
		t.Fatalf("новая минута закрылась раньше времени: %+v", rows)
	}
	rows = aggregator.Apply(QuotationEvent{
		SecID: 1, Board: "TQBR", SecCode: "SBER", Time: "10:02:00", Last: 104,
	}, now)
	if len(rows) != 1 {
		t.Fatalf("вторая минута не закрыта: %+v", rows)
	}
	if rows[0].Open != 102 || rows[0].Close != 104 || rows[0].High != 103 || rows[0].Low != 103 {
		t.Fatalf("вторая свеча = %+v", rows[0])
	}
}

func TestQuotationAggregatorKeepsSecCode(t *testing.T) {
	aggregator := newQuotationAggregator()
	now := time.Date(2026, time.August, 14, 10, 1, 0, 0, moscowLocation)
	aggregator.Apply(QuotationEvent{SecID: 1, SecCode: "SBER", Time: "10:00:10", Last: 100, Quantity: 1}, now)
	rows := aggregator.Apply(QuotationEvent{SecID: 1, Time: "10:01:00", Last: 101}, now)
	if len(rows) != 1 || rows[0].SecCode != "SBER" {
		t.Fatalf("sec_code потерян: %+v", rows)
	}
}

func TestQuotationAggregatorZeroLastDoesNotClose(t *testing.T) {
	aggregator := newQuotationAggregator()
	now := time.Date(2026, time.August, 14, 10, 1, 0, 0, moscowLocation)
	aggregator.Apply(QuotationEvent{SecID: 1, SecCode: "SBER", Time: "10:00:10", Last: 100, Quantity: 1}, now)
	if rows := aggregator.Apply(QuotationEvent{SecID: 1, Time: "10:01:00"}, now); len(rows) != 0 {
		t.Fatalf("нулевая last не должна закрывать минуту: %+v", rows)
	}
}
