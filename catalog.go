package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	tcClient "github.com/kmlebedev/txmlconnector/client"
	"github.com/kmlebedev/txmlconnector/client/commands"
)

// securitiesCatalog накапливает каталог инструментов.
//
// Transaq отдаёт каталог несколькими сообщениями `securities`, а клиент хранит
// только последнее: `TCClient.Data.Securities` заменяется целиком (так же, как
// раньше вёл себя encoding/xml, но теперь без append). Подписка, построенная по
// последнему сообщению, содержит единицы инструментов — фильтр EXPORT_SEC_BOARDS
// не находит ни одной доски и `subscribe` уходит с пустым `<alltrades>`, на что
// сервер отвечает «Неверный формат команды».
//
// Поэтому каталог собирается из типизированного потока PortfolioUpdates
// (Kind = UpdateSecurities) с дедупликацией по secid и опорой на время последнего
// сообщения: подписка отправляется, когда поток стих.
type securitiesCatalog struct {
	mu     sync.Mutex
	order  []commands.Security
	seen   map[int]struct{}
	lastAt time.Time
}

func newSecuritiesCatalog() *securitiesCatalog {
	return &securitiesCatalog{seen: make(map[int]struct{})}
}

// add дописывает инструменты порции, пропуская уже виденные secid: повторный
// connect присылает каталог заново, и без дедупликации подписка разрослась бы
// дублями secid.
func (c *securitiesCatalog) add(items []commands.Security) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, item := range items {
		if _, ok := c.seen[item.SecId]; ok {
			continue
		}
		c.seen[item.SecId] = struct{}{}
		c.order = append(c.order, item)
	}
	c.lastAt = time.Now()
}

// snapshot отдаёт копию накопленного каталога.
func (c *securitiesCatalog) snapshot() []commands.Security {
	c.mu.Lock()
	defer c.mu.Unlock()
	items := make([]commands.Security, len(c.order))
	copy(items, c.order)
	return items
}

func (c *securitiesCatalog) size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.order)
}

func (c *securitiesCatalog) lastUpdate() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastAt
}

// waitQuiet ждёт, пока поток каталога стихнет: последняя порция пришла не позже
// quiet назад. Пустой каталог — ошибка: подписка по нему ушла бы пустой.
func (c *securitiesCatalog) waitQuiet(ctx context.Context, quiet, timeout time.Duration) error {
	if quiet <= 0 {
		return nil
	}
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(quiet / 5)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case now := <-ticker.C:
			if c.size() == 0 {
				if now.After(deadline) {
					return fmt.Errorf("каталог securities не пришёл за %s: подписка была бы пустой", timeout)
				}
				continue
			}
			if now.Sub(c.lastUpdate()) >= quiet {
				return nil
			}
			if now.After(deadline) {
				// Каталог уже непустой: подписываемся на увиденное, а не падаем.
				return nil
			}
		}
	}
}

// startSecuritiesCatalogWorker переносит порции каталога из типизированного
// потока в накопитель. Без стрима (Options.PortfolioBuffer = 0) рабочий горутин
// не нужен: остаётся путь через Data.Securities.
func startSecuritiesCatalogWorker(ctx context.Context, client *tcClient.TCClient, catalog *securitiesCatalog) {
	if catalog == nil {
		return
	}
	updates := client.PortfolioUpdates()
	if updates == nil {
		return
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case update, ok := <-updates:
				if !ok {
					return
				}
				if update.Kind != tcClient.UpdateSecurities || update.Securities == nil {
					continue
				}
				catalog.add(update.Securities.Items)
			}
		}
	}()
}
