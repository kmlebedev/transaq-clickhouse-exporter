package main

import (
	"context"
	"testing"
	"time"
)

// Каталог приходит несколькими сообщениями securities: накопитель обязан
// склеить порции без дублей по secid — иначе подписка строится по последней
// порции (наблюдалось 2026-10-30: 9 инструментов и пустой `<alltrades>`).
func TestSecuritiesCatalogAccumulatesWithoutDuplicates(t *testing.T) {
	catalog := newSecuritiesCatalog()
	catalog.add([]Security{{SecID: 1, SecCode: "SBER", Board: "TQBR"}})
	catalog.add([]Security{
		{SecID: 2, SecCode: "SU26238RMFS9", Board: "TQOB"},
		{SecID: 1, SecCode: "SBER", Board: "TQBR"}, // повтор после reconnect
	})

	items := catalog.snapshot()
	if len(items) != 2 {
		t.Fatalf("инструментов = %d, want 2: %+v", len(items), items)
	}
	if items[0].SecCode != "SBER" || items[1].SecCode != "SU26238RMFS9" {
		t.Fatalf("порядок порций потерян: %+v", items)
	}
	if catalog.size() != 2 {
		t.Fatalf("size = %d, want 2", catalog.size())
	}

	catalog.add([]Security{{SecID: 3, SecCode: "GAZP", Board: "TQBR"}})
	items = catalog.snapshot()
	items[0].SecCode = "mutated"
	if catalog.snapshot()[0].SecCode != "SBER" {
		t.Fatal("snapshot отдаёт внутренний срез: правка копии меняет каталог")
	}
}

// waitQuiet — подписка отправляется, когда поток каталога стих; пустой каталог
// не подписывается вовсе.
func TestSecuritiesCatalogWaitQuiet(t *testing.T) {
	t.Run("стихший поток", func(t *testing.T) {
		catalog := newSecuritiesCatalog()
		catalog.add([]Security{{SecID: 1, SecCode: "SBER"}})
		time.Sleep(200 * time.Millisecond)
		if err := catalog.waitQuiet(context.Background(), 100*time.Millisecond, 2*time.Second); err != nil {
			t.Fatalf("waitQuiet: %v", err)
		}
	})

	t.Run("пустой каталог — ошибка", func(t *testing.T) {
		catalog := newSecuritiesCatalog()
		err := catalog.waitQuiet(context.Background(), 50*time.Millisecond, 300*time.Millisecond)
		if err == nil {
			t.Fatal("waitQuiet вернул nil на пустом каталоге: подписка ушла бы пустой")
		}
	})

	t.Run("живой поток не ждёт таймаут", func(t *testing.T) {
		catalog := newSecuritiesCatalog()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		done := make(chan error, 1)
		go func() {
			done <- catalog.waitQuiet(ctx, 100*time.Millisecond, 2*time.Second)
		}()
		for i := 0; i < 5; i++ {
			time.Sleep(20 * time.Millisecond)
			catalog.add([]Security{{SecID: i + 1, SecCode: "SBER"}})
		}
		if err := <-done; err != nil {
			t.Fatalf("waitQuiet: %v", err)
		}
	})
}
