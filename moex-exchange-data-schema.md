# Схема данных MOEX через Transaq

Документ описывает фактический конвейер `transaq-clickhouse-exporter` и его
источник [`txmlconnector`](https://github.com/kmlebedev/txmlconnector). Это
MOEX-аналог [`exchange-data-schema.md`](exchange-data-schema.md): вместо
WebSocket → Valkey → `crypto.*` здесь используется Transaq XML Connector →
gRPC → ClickHouse → таблицы `transaq_*`.

Главное правило чтения: **наличие Go-структуры или таблицы ещё не означает, что
данные реально собираются**. Ниже отдельно отмечены активные подписки,
неподключённые обработчики и производные, которые ещё предстоит реализовать.

## Содержание

1. [Архитектура и матрица сбора](#1-архитектура-и-матрица-сбора)
   - [Конвейер](#11-конвейер)
   - [Что происходит с сообщениями](#12-что-происходит-с-сообщениями)
   - [Соответствие категориям crypto-ленты](#13-соответствие-категориям-crypto-ленты)
2. [Контракты входящих сообщений](#2-контракты-входящих-сообщений)
   - [Тикерные показатели quotations](#21-тикерные-показатели-quotations)
   - [Публичные сделки alltrades](#22-публичные-сделки-alltrades)
   - [Стакан quotes](#23-стакан-quotes)
   - [Свечи candles](#24-свечи-candles)
   - [Каталог securities](#25-каталог-securities)
   - [Расширенные сведения sec_info и sec_info_upd](#26-расширенные-сведения-sec_info-и-sec_info_upd)
   - [Позиции и портфель](#27-позиции-и-портфель)
3. [Поведение конвейера](#3-поведение-конвейера)
4. [ClickHouse: таблицы](#4-clickhouse-таблицы)
   - [`transaq_trades`](#41-transaq_trades)
   - [`transaq_quotes`](#42-transaq_quotes)
   - [`transaq_candles`](#43-transaq_candles)
   - [`transaq_securities`](#44-transaq_securities)
   - [`transaq_securities_info`](#45-transaq_securities_info)
5. [Юниверс инструментов и фильтры](#5-юниверс-инструментов-и-фильтры)
6. [Чего в ленте нет](#6-чего-в-ленте-нет)
7. [Как проверить, что сбор жив](#7-как-проверить-что-сбор-жив)
8. [Правила изменений](#8-правила-изменений)

## 1. Архитектура и матрица сбора

### 1.1 Конвейер

```text
MOEX / брокерский Transaq-сервер
        |
        | XML-команды и broadcast XML
        v
TXmlConnector.dll (Win32, вызовы сериализуются)
        |
        | Windows natively или Wine на Linux
        v
txmlconnector server
        |
        | gRPC: SendCommand + FetchResponseData
        v
txmlconnector Go client
        |
        | typed channels + legacy Data/ResponseChannel
        v
transaq-clickhouse-exporter
        |
        | native protocol, batch INSERT / AsyncInsert
        v
ClickHouse: transaq_trades, transaq_quotes, transaq_candles,
            transaq_securities, transaq_securities_info
```

DLL распространяется как Win32-бинарник, поэтому нативный Linux-клиент не
загружает её напрямую. gRPC-сервер работает на Windows или под Wine, а экспортёр
остаётся кроссплатформенным Go-приложением. Подробности границ компонентов —
в `https://github.com/kmlebedev/txmlconnector/docs/architecture.md`.

gRPC-endpoint `txmlconnector` не аутентифицирует клиентов. Для не локального
подключения нужен VPN или SSH-туннель. Учётные данные Transaq отправляет клиент
в команде `connect`; они не должны попадать в документы, SQL-примеры или логи
схемы данных.

Valkey в этом контуре **нет**: последнее состояние и история не разделены на два
хранилища, всё сохраняемое экспортёром записывается непосредственно в
ClickHouse.

### 1.2 Что происходит с сообщениями

| XML-сообщение | Назначение | Путь в клиенте | Что делает экспортёр | Статус записи |
| ------------- | ---------- | -------------- | -------------------- | ------------- |
| `alltrades` | публичные сделки | `AllTradesChan` | пакетный `INSERT` | **`transaq_trades`** |
| `quotations` | тикерные показатели, BBO, дневная статистика | `Data.Quotations` + `ResponseChannel` | агрегирует минутные свечи | только производное в **`transaq_candles`**; сырого тикера нет |
| `quotes` | инкрементальные строки стакана | `QuotesChan` | пакетный `INSERT` | обработчик и **`transaq_quotes`** есть, но текущая подписка не отправляется |
| `candles` | история свечей | `Data.Candles` + `ResponseChannel` | пакетный `INSERT` | **`transaq_candles`**, если включён `EXPORT_CANDLE_COUNT` |
| `securities` | базовый каталог инструментов | `Data.Securities` | читает при восстановлении подписок | активные записи в **`transaq_securities`** |
| `sec_info` | расширенный справочник инструмента | `SecInfoChan` | `AsyncInsert` | **`transaq_securities_info`** по фильтру |
| `sec_info_upd` | обновление лимитов/ГО инструмента | `SecInfoUpdChan` | debug-лог | **не записывается** |
| `positions` | деньги, бумаги, FORTS, лимиты | `Data.Positions` + `ResponseChannel` | кэширует части, логирует, дополняет список сделок `positions` | **не записывается** |
| `united_portfolio` | единый портфель, обеспечение, PnL | `Data.UnitedPortfolio` + `ResponseChannel` | info-лог | **не записывается** |
| `united_equity` | текущая оценка единого портфеля | `Data.UnitedEquity` + `ResponseChannel` | info-лог | **не записывается** |
| `server_status` | состояние соединения терминала | `ServerStatusChan` | запускает восстановление или reconnect | не рыночные данные |

Активная команда `subscribe` в `process.go` содержит только `Quotations` и
`AllTrades`. Поле `Command.Quotes` в `txmlconnector` существует, но экспортёр
его не заполняет. Поэтому пустая `transaq_quotes` при штатной конфигурации —
ожидаемое состояние, а не признак отсутствия заявок на MOEX.

### 1.3 Соответствие категориям crypto-ленты

| Категория из `exchange-data-schema.md` | Источник MOEX/Transaq | Текущий результат |
| -------------------------------------- | --------------------- | ----------------- |
| Тикер / BBO | `quotations`: `last`, `bid`, `biddepth`, `offerdepth`, `voltoday`, `valtoday`, `openpositions` и др. | отдельной тикерной таблицы нет; сообщения участвуют в сборке минутных свечей |
| Стакан L2 | `quotes`: цена, источник, buy/sell, удаление `-1` | есть сырой инкрементальный формат и таблица, но подписка не включена; готовых snapshot L2 нет |
| OFI | можно рассчитывать после восстановления последовательных снимков книги | не рассчитывается |
| Сырая лента сделок | `alltrades` | сохраняется в `transaq_trades` |
| CVD | база для расчёта: `price`, `quantity`, `buysell` в `alltrades` | не рассчитывается и не сохраняется |
| Балансы | `money_position`, `forts_money`, `forts_collaterals`, `united_limits`, `united_portfolio` | принимаются/логируются, в ClickHouse не пишутся |
| Ledger: fills | публичные `alltrades` — **не** собственные исполнения аккаунта | отдельного потока собственных fills и таблицы нет |
| Funding | у MOEX нет аналога hourly funding perpetual futures из исходного документа | не собирается; вариационная маржа FORTS — другой финансовый поток |
| Каталог инструментов | `securities` + `sec_info` + `sec_info_upd` | первые два сохраняются; updates только логируются |
| Исторические свечи | `gethistorydata` → `candles` | сохраняются при включении |

Нельзя подменять эти категории похожими по названию полями. Например,
`openpositions`/`openinterest` — открытый интерес FORTS, `varmargin` —
вариационная маржа, а `alltrades.buysell` — признак стороны публичной сделки,
а не запись о собственном ордере аккаунта.

## 2. Контракты входящих сообщений

Исходные контракты — Go-структуры в `txmlconnector/client/commands/main.go`.
XML декодируется стандартным `encoding/xml`. В отличие от crypto-контура, цены и
объёмы здесь **не являются десятичными строками**: после разбора это `float64`,
`float32`, `int`, `int64` и связанные типы.

У числовых полей нет указателей/`Nullable`: отсутствующий XML-элемент после
`xml.Unmarshal` остаётся нулевым значением Go. Поэтому на уровне текущей схемы
нельзя всегда отличить «источник не прислал поле» от настоящего нуля.

### 2.1 Тикерные показатели `quotations`

Условный пример сообщения:

```xml
<quotations>
  <quotation secid="36116">
    <board>FUT</board>
    <seccode>SiU9</seccode>
    <last>63384</last>
    <bid>63383</bid>
    <biddepth>38</biddepth>
    <offerdepth>33</offerdepth>
    <voltoday>519036</voltoday>
    <numtrades>77015</numtrades>
    <valtoday>32873.706</valtoday>
    <openpositions>2273956</openpositions>
    <time>15:04:05</time>
  </quotation>
</quotations>
```

| Поле | Go-тип | Смысл / единицы |
| ---- | ------ | --------------- |
| `secid` | `int` | внутренний идентификатор Transaq |
| `board`, `seccode` | `string` | режим торгов и код инструмента |
| `open`, `last`, `high`, `low` | `float64` | цены первой/последней сделки и экстремумы |
| `waprice` | `float64` | средневзвешенная цена |
| `quantity` | `int` | объём последней сделки, **лоты** |
| `time` | `string` | время последней сделки |
| `bid` | `float64` | лучшая цена покупки |
| `biddepth`, `offerdepth` | `int` | объём на лучшей цене, **лоты** |
| `biddeptht`, `offerdeptht` | `int` | совокупный спрос/предложение |
| `numbids`, `numoffers` | `int` | число заявок покупки/продажи |
| `numtrades` | `int` | число сделок |
| `voltoday` | `int` | дневной объём сделок, **лоты** |
| `valtoday` | `float64` | дневной оборот, **млн руб.** |
| `openpositions`, `deltapositions` | `int` | открытые позиции FORTS и их изменение |
| `change`, `priceminusprevwaprice` | `float64` | изменение к прошлому дню/оценке |
| `yield`, `yieldatwaprice`, `closeyield` | `float64` | доходности для применимых инструментов |
| `marketpricetoday`, `closeprice`, `lcurrentprice` | `float64` | рыночные/официальные цены |
| `status`, `tradingstatus` | `string` | разрешение операций и состояние сессии |
| `buydeposit`, `selldeposit`, `bgo_buy` | `float64` | гарантийное обеспечение |
| `theoreticalprice`, `volatility`, `point_cost` | `float64` | параметры производных инструментов |

В структуре также есть `highbid`, `lowoffer`, `priceMinusPrevwaPrice` и другие
поля; полный список должен синхронизироваться с `commands.Quotation`.

Экспортёр не сохраняет этот кадр как тикер. Он поддерживает в памяти
`quotationCandles[secid]`: открывает минуту по первому значению, обновляет
high/low и суммирует `quantity`, а на сообщении с временем, оканчивающимся на
`:00`, записывает свечу с `period=1`. Следовательно, `transaq_candles` нельзя
использовать как полную историю BBO или всех полей `quotations`.

### 2.2 Публичные сделки `alltrades`

```xml
<alltrades>
  <trade secid="36116">
    <seccode>SiU9</seccode>
    <tradeno>981234567</tradeno>
    <time>29.10.2026 15:04:05</time>
    <board>FUT</board>
    <price>63384</price>
    <quantity>3</quantity>
    <buysell>B</buysell>
    <openinterest>2273956</openinterest>
    <period>N</period>
  </trade>
</alltrades>
```

| Поле | ClickHouse-поле | Смысл |
| ---- | --------------- | ----- |
| `secid` | `secid` | внутренний id инструмента |
| `seccode` | `sec_code` | код инструмента |
| `tradeno` | `trade_no` | биржевой номер сделки |
| `time` | `time` | `DD.MM.YYYY HH:MM:SS`, секундная точность |
| `board` | `board` | режим торгов |
| `price` | `price` | цена сделки |
| `quantity` | `quantity` | объём, **лоты**, не базовые бумаги |
| `buysell` | `buy_sell` | `B` — покупка, `S` — продажа |
| `openinterest` | `open_interest` | открытый интерес после/на момент сделки, где применимо |
| `period` | `period` | `O` — открытие, `N` — торги, `C` — закрытие |

Одно XML-сообщение может содержать несколько сделок. `insertTrades` добавляет их
в один ClickHouse batch и отправляет одним `Send()`. Ошибки парсинга времени в
текущей реализации игнорируются; это не превращает схему в Nullable-контракт.

CVD из этих данных можно построить отдельным потребителем: знак дельты берётся
из `buy_sell`, размер — из `quantity`. Но сам экспортёр таких накопителей не
ведёт, а правила сессионности и восстановления CVD после reconnect пока не
определены.

### 2.3 Стакан `quotes`

```xml
<quotes>
  <quote secid="4917">
    <seccode>SU26207RMFS9</seccode>
    <board>TQOB</board>
    <price>99.85</price>
    <source>MM</source>
    <yield>0</yield>
    <buy>120</buy>
    <sell>-1</sell>
  </quote>
</quotes>
```

| Поле | Смысл |
| ---- | ----- |
| `secid`, `seccode`, `board` | идентификация инструмента и режима |
| `price` | цена уровня |
| `source` | источник котировки, например маркетмейкер |
| `yield` | доходность для облигаций; в Go `int`, в ClickHouse `Int8` |
| `buy` | количество к покупке; `-1` — заявок на покупку больше нет |
| `sell` | количество к продаже; `-1` — заявок на продажу больше нет |

`buy=-1` и `sell=-1` одновременно означают удаление строки с данной ценой или
парой `(price, source)`. Это **дельта книги**, а не самостоятельный снимок L2:
для получения книги потребителю нужны начальное состояние, последовательное
применение изменений и контроль пропусков.

У `commands.Quotes` дополнительно есть `Time time.Time`. Оно заполняется клиентом
как `time.Now()` в `Europe/Moscow` при разборе сообщения. В `transaq_quotes.time`
поэтому лежит **локальное время приёма**, а не биржевая метка изменения уровня.

### 2.4 Свечи `candles`

```xml
<candles secid="36116" board="FUT" seccode="SiU9" period="60" status="0">
  <candle date="29.10.2026 15:00:00"
          open="63370" close="63384" high="63390" low="63360"
          volume="1250"/>
</candles>
```

Атрибуты контейнера:

- `secid`, `board`, `seccode` — инструмент;
- `period` — идентификатор периода из `candlekinds`, не всегда число минут;
- `status`: `0` — данных больше нет, `1` — заказанное количество выдано,
  `2` — будет продолжение, `3` — данные временно недоступны.

Каждый `candle` содержит `date`, `open`, `close`, `high`, `low`, `volume`.
Исторические сообщения возникают после `gethistorydata`. Отдельный путь —
минутная агрегация `quotations`, описанная в §2.1. В таблице оба источника
оказываются вместе, но поля `board` и признак источника не сохраняются.

### 2.5 Каталог `securities`

`securities` приходит как снимок списка инструментов после подключения. Для
подписок и справочника используются:

| Поле | Смысл |
| ---- | ----- |
| `secid`, `active` | внутренний id и признак активности |
| `seccode`, `instrclass`, `board`, `market` | код, класс, основной режим и рынок |
| `shortname` | краткое имя |
| `decimals`, `minstep` | точность отображения и минимальный шаг цены |
| `lotsize`, `lotdivider` | размер лота и делитель лота |
| `point_cost` | стоимость пункта |
| `sectype` | тип инструмента, например `BOND` |
| `quotestype` | `0` — без стакана, `1` — OrderBook, `2` — Level2 |
| `opmask` | допустимые торговые операции; не сохраняется текущей таблицей |
| `sec_tz`, `MIC`, `ticker` | дополнительные атрибуты; не сохраняются текущей таблицей |

В `transaq_securities` попадают только записи с `secid != 0`, `active="true"` и
`len(seccode) <= 16`. Фильтр `EXPORT_SEC_BOARDS` ограничивает подписки, но **не**
ограничивает запись базового каталога: экспортёр сначала пытается сохранить все
активные инструменты, прошедшие эти три проверки.

### 2.6 Расширенные сведения `sec_info` и `sec_info_upd`

`get_securities_info` запрашивается либо по `secid`, либо по паре
`market + seccode`. `sec_info` содержит:

- идентификацию: `secid`, `secname`, `seccode`, `market`, `isin`, `regnumber`;
- параметры цены: `pname`, `clearing_price`, `minprice`, `maxprice`,
  `point_cost`, `currencyid`;
- ГО FORTS: `buy_deposit`, `sell_deposit`, `bgo_c`, `bgo_nc`, `bgo_buy`;
- облигации: `mat_date`, `accruedint`, `coupon_value`, `coupon_date`,
  `coupon_period`, `facevalue`, `buybackprice`, `buybackdate`;
- опционы/базовый актив: `put_call`, `opt_type`, `lot_volume`.

Даты `mat_date`, `coupon_date`, `buybackdate` разбираются по `DD.MM.YYYY` и
записываются как `DateTime('Europe/Moscow')`, хотя по смыслу это календарные
даты. Ошибки разбора дат, как и времени сделок, текущий код не проверяет.

`sec_info_upd` содержит `secid`, `market`, `seccode`, лимиты цены, ГО и
`point_cost`. Эти обновления не применяются к `transaq_securities_info`:
обработчик только пишет debug-лог. Таблица обновляется следующим явным ответом
`sec_info`, а не потоком `sec_info_upd`.

### 2.7 Позиции и портфель

Доступные структуры покрывают другой класс данных, чем публичная лента:

| Структура | Основные поля |
| --------- | ------------- |
| `MoneyPosition` | `saldoin`, `bought`, `sold`, `saldo`, `ordbuy`, `currency`, `asset`, `comission` |
| `SecPosition` | `secid`, `seccode`, `saldo`, `ordbuy`, `amount`, `equity` |
| `FortsPosition` | `startnet`, `totalnet`, `todaybuy`, `todaysell`, `varmargin`, открытые заявки |
| `FortsMoney` / `FortsCollaterals` | `current`, `blocked`, `free`, у FORTS money ещё `varmargin` |
| `SpotLimit` | `buylimit`, `buylimitused` |
| `UnitedLimits` | `open_equity`, `equity`, `requirements`, `free`, `vm`, `finres`, `go` |
| `UnitedPortfolio` | денежные части, активы, нетто-позиции, `unrealized_pnl`, требования и обеспечение |

Экспортёр частично кэширует `positions`, логирует `positions`,
`united_portfolio` и `united_equity`. При `EXPORT_ALL_TRADES=positions` код
добавляет `secid` из `SecPositions` в подписку публичных сделок. Это не делает
сделки собственными fills: в `transaq_trades` по-прежнему идёт рыночная лента.

## 3. Поведение конвейера

### Подключение и восстановление

1. Клиент открывает gRPC response stream и ждёт readiness headers.
2. Только после этого отправляется `connect`, чтобы начальные снимки не были
   опубликованы до появления подписчика.
3. При `server_status.connected="true"` экспортёр один раз за сессию вызывает
   `restoreSubscriptions`.
4. Восстановление очищает незавершённые `quotationCandles`, заново читает
   `securities`, формирует подписки и отправляет `get_securities_info`.
5. `connected="false"`/`"error"`, закрытие response stream или ошибка
   восстановления завершают сессию. `RunWithReconnect` создаёт новый клиент с
   экспоненциальной задержкой от 1 до 30 с; после минуты стабильной сессии
   задержка снова начинается с минимальной.

Очистка `quotationCandles` намеренно предотвращает склейку минуты с пробелом во
входном потоке. Она не восстанавливает пропущенные рыночные события.

### Очереди и ClickHouse

- `alltrades`, `quotes`, `sec_info`, `sec_info_upd` обрабатываются независимыми
  workers. Небольшие каналы клиента дренируются во внутренние очереди, пока
  ClickHouse или восстановление подписок работают медленно.
- При длине очереди 1024, затем 2048, 4096 и т. д. пишется предупреждение
  `TRANSAQ ... queue reached ... events`.
- Сделки и quotes записываются batch'ами; `sec_info` — через `AsyncInsert` с
  `async_insert_wait=false`.
- Схема из пяти таблиц создаётся через `CREATE TABLE IF NOT EXISTS` при старте,
  до подключения к Transaq. Миграционного журнала, как в crypto-контуре, нет.
- Подключение к ClickHouse повторяется до 10 раз с паузой 3 с. DSN по умолчанию
  `tcp://127.0.0.1:9000`, переопределяется `CLICKHOUSE_URL`.

Очереди находятся в памяти. Они не дают exactly-once, не переживают остановку
процесса и не исключают дублей после повторных запросов/подписок. Ошибка одного
INSERT логируется worker'ом; универсального механизма повторной записи batch в
текущем коде нет.

## 4. ClickHouse: таблицы

Все пять таблиц создаются в текущей базе из `CLICKHOUSE_URL`; префикс `crypto.`
или отдельная БД `moex` в DDL не указаны. Все используют
`ReplacingMergeTree()` **без версии**, явных TTL и колонки `ingested_at`.

`ReplacingMergeTree` не гарантирует немедленную дедупликацию после INSERT:
дубликаты возможны до background merge. Для снимочных справочников используйте
`FINAL` или агрегацию по ключу сортировки, если запрос должен видеть одну версию.
Для `transaq_quotes` ключ сортировки дополнительно означает, что это не надёжный
архив всех однотипных изменений одного уровня: время и объёмы в ключ не входят.

| Таблица | Источник | Ключ сортировки | Фактическая заполняемость |
| ------- | -------- | --------------- | ------------------------- |
| `transaq_trades` | `alltrades` | `(secid, board, sec_code, trade_no, time, buy_sell)` | при настроенной `EXPORT_ALL_TRADES` |
| `transaq_quotes` | `quotes` | `(sec_code, board, price, source)` | обработчик есть, подписка сейчас не отправляется |
| `transaq_candles` | `candles` и агрегация `quotations` | `(date, sec_code, period)` | при включённой истории и/или подписке quotations |
| `transaq_securities` | `securities` | `(seccode, instrclass, board, market, sectype, quotestype)` | при восстановлении сессии, для активных инструментов |
| `transaq_securities_info` | `sec_info` | `(sec_code, market, regnumber, isin)` | по фильтру `EXPORT_SEC_INFO_NAMES`, только `BOND` |

### 4.1 `transaq_trades`

```sql
CREATE TABLE IF NOT EXISTS transaq_trades (
    time   DateTime('Europe/Moscow'),
    secid   UInt16,
    sec_code LowCardinality(FixedString(16)),
    trade_no Int64,
    board LowCardinality(String),
    price   Float32,
    quantity UInt32,
    buy_sell LowCardinality(FixedString(1)),
    open_interest Int32,
    period LowCardinality(FixedString(1))
) ENGINE = ReplacingMergeTree()
ORDER BY (secid, board, sec_code, trade_no, time, buy_sell)
```

Это полная сохраняемая лента публичных сделок по выбранным инструментам, а не
CVD-снимки и не собственные исполнения. `quantity` — лоты; для денежного объёма
нужны правила лота из каталога и валюта/единица цены инструмента.

### 4.2 `transaq_quotes`

```sql
CREATE TABLE IF NOT EXISTS transaq_quotes (
    time     DateTime('Europe/Moscow'),
    secid    UInt16,
    board    LowCardinality(String),
    sec_code LowCardinality(FixedString(16)),
    price    Float32,
    source   LowCardinality(String),
    yield    Int8,
    buy      Int16,
    Sell     Int16,
) ENGINE = ReplacingMergeTree()
ORDER BY (sec_code, board, price, source)
```

Имя колонки именно `Sell` с заглавной `S` — так задан текущий DDL. `time` —
время приёма сообщения клиентом. Поскольку `secid`, `time`, `buy`, `Sell` и
`yield` не входят в ключ, разные обновления одного `(sec_code, board, price,
source)` могут быть схлопнуты merge'ем. Для сырого журнала дельт или L2-истории
потребуется отдельная схема, а не только включение подписки.

### 4.3 `transaq_candles`

```sql
CREATE TABLE IF NOT EXISTS transaq_candles (
    date   DateTime('Europe/Moscow'),
    sec_code FixedString(16),
    period UInt16,
    open   Float32,
    close  Float32,
    high   Float32,
    low    Float32,
    volume UInt64
) ENGINE = ReplacingMergeTree()
ORDER BY (date, sec_code, period)
```

В таблице нет `board`, `secid` и типа источника. Одинаковые `sec_code` на разных
режимах могут попадать в один ключ. `period` из истории — id `candlekinds`;
минутные свечи из quotations записываются с `period=1`.

### 4.4 `transaq_securities`

```sql
CREATE TABLE IF NOT EXISTS transaq_securities (
    secid   UInt16,
    seccode FixedString(16),
    instrclass String,
    board String,
    market UInt8,
    shortname String,
    decimals UInt8,
    minstep Float32,
    lotsize UInt32,
    lotdivider UInt16,
    point_cost Float32,
    sectype String,
    quotestype UInt8
) ENGINE = ReplacingMergeTree()
ORDER BY (seccode, instrclass, board, market, sectype, quotestype)
```

Это последний экспортированный снимок базовых правил, а не история изменений с
`as_of`. При чтении инструменты следует связывать по составу `(secid, seccode,
board)`; один тикер сам по себе не всегда является достаточным ключом.

### 4.5 `transaq_securities_info`

```sql
CREATE TABLE IF NOT EXISTS transaq_securities_info (
    secid   UInt16,
    sec_name String,
    sec_code FixedString(16),
    market UInt8,
    pname  String,
    mat_date DateTime('Europe/Moscow'),
    clearing_price Float32,
    minprice Float32,
    maxprice Float32,
    buy_deposit Float32,
    sell_deposit Float32,
    bgo_c Float32,
    bgo_nc Float32,
    bgo_buy Float32,
    accruedint Float32,
    coupon_value Float32,
    coupon_date DateTime('Europe/Moscow'),
    coupon_period UInt16,
    facevalue Float32,
    put_call FixedString(1),
    point_cost Float32,
    opt_type FixedString(1),
    lot_volume UInt32,
    isin String,
    regnumber String,
    buybackprice Float32,
    buybackdate DateTime('Europe/Moscow'),
    currencyid String
) ENGINE = ReplacingMergeTree()
ORDER BY (sec_code, market, regnumber, isin)
```

Здесь нет времени снимка; это текущее состояние расширенного справочника по
ключу, а не временной ряд ГО, НКД или лимитов. `sec_info_upd` историю не
формирует (§2.6).

## 5. Юниверс инструментов и фильтры

| Переменная | Что делает | Поведение по умолчанию / особенности |
| ---------- | ---------- | ------------------------------------ |
| `EXPORT_SEC_BOARDS` | список режимов для quotations/alltrades и истории | в коде `TQBR,TQCB,FUT`; README и пример service указывают `TQBR,TQTF,FUT` — фактический default определяет `main.go` |
| `EXPORT_SEC_CODES` | коды/подстроки кодов, shortname или `ALL` для quotations и истории | пусто = подписок quotations и истории нет |
| `EXPORT_ALL_TRADES` | `ALL`, список точных `seccode` и/или `positions` | пусто = публичные сделки не запрашиваются; `positions` добавляет инструменты из текущих позиций |
| `EXPORT_SEC_INFO_NAMES` | `ALL` или суффиксы `shortname` для запроса `sec_info` | применяется только к `sectype=BOND`; в текущем цикле не ограничивается выбранными boards |
| `EXPORT_CANDLE_COUNT` | число исторических свечей на инструмент/period | `0` — история выключена; положительное значение запрашивает count, отрицательное включает режим дочитывания истории |
| `EXPORT_PERIOD_SECONDS` | фильтр `candlekinds.period` для истории | пусто = все доступные периоды; не меняет отдельную минутную агрегацию quotations |
| `CLICKHOUSE_URL` | DSN ClickHouse | `tcp://127.0.0.1:9000` |
| `LOG_LEVEL` | уровень логов экспортёра | стандартный уровень logrus, если значение не задано/неверно |

Пример полного рыночного сбора по выбранным режимам:

```bash
EXPORT_SEC_BOARDS=TQBR,TQCB,TQOB,FUT
EXPORT_SEC_CODES=ALL
EXPORT_ALL_TRADES=ALL
EXPORT_SEC_INFO_NAMES=ALL
EXPORT_CANDLE_COUNT=100
EXPORT_PERIOD_SECONDS=60
```

Пример выборочного сбора ОФЗ:

```bash
EXPORT_SEC_BOARDS=TQCB,TQOB
EXPORT_SEC_CODES=ALL
EXPORT_ALL_TRADES=ALL
EXPORT_SEC_INFO_NAMES=ALL
```

Практические правила:

- Сначала сверяйте `quotestype`: он показывает, доступен ли для инструмента
  OrderBook/Level2, но сам не включает подписку `quotes`.
- `secid` нужен для XML-подписок, `seccode` — для фильтров и чтения таблиц,
  `board` — для различения режимов. Храните соответствие всех трёх.
- Фильтр `EXPORT_SEC_CODES` использует точное совпадение, `shortname`,
  `ALL` **или substring** `seccode`; короткий фрагмент может выбрать больше
  инструментов, чем ожидалось. `EXPORT_ALL_TRADES` для списка кодов использует
  точное совпадение.
- Команда может вернуть `success=true`, но брокерский аккаунт всё равно может
  не отдавать часть рыночных данных. Проверка выполняется по фактическим строкам
  и свежести, а не только по результату подписки.

## 6. Чего в ленте нет

| Чего нет | Почему / следствие |
| -------- | ------------------ |
| Отдельной таблицы сырых тикеров/BBO | `quotations` не сериализуется в ClickHouse напрямую, используется для минутных свечей |
| Активного сбора `transaq_quotes` | `restoreSubscriptions` не заполняет `Command.Quotes` |
| Готовых снимков L2 fast/deep | Transaq `quotes` — дельты; сборка книги не реализована |
| OFI | нет восстановленных последовательных снимков и расчёта |
| CVD, buyVol, sellVol | есть исходные `alltrades`, но накопители не реализованы |
| Балансов и позиций в ClickHouse | `positions`/portfolio только кэшируются или логируются |
| Собственных ордеров/fills и комиссий по ним | текущий typed-client/экспортёр не обрабатывает такой ledger; публичные сделки нельзя считать своими |
| Funding | неприменим в crypto-смысле; `varmargin`/`finres` FORTS требуют собственной модели |
| Истории `sec_info_upd` | updates только логируются |
| Исторических версий справочников | нет `as_of`; `ReplacingMergeTree` хранит заменяемое состояние |
| Миллисекундных биржевых timestamp в сделках | текущий layout `DD.MM.YYYY HH:MM:SS` имеет секундную точность |
| Биржевого timestamp у quotes | сохраняется время локального приёма |
| Nullable-семантики отсутствующих XML-полей | числовые Go-поля не являются указателями, ClickHouse-колонки не Nullable |
| Явных TTL и `ingested_at` | отсутствуют во всех пяти DDL |
| Exactly-once и гарантированного backfill после reconnect | очереди в памяти, подписки восстанавливаются, пропущенный рыночный поток не догоняется |
| Немедленной дедупликации | `ReplacingMergeTree` схлопывает ключи в background merge |

## 7. Как проверить, что сбор жив

Время в таблицах — `Europe/Moscow`. Отсутствие строк вне торговой сессии не
обязательно означает поломку; сопоставляйте проверку с расписанием выбранных
режимов.

```sql
-- Публичные сделки за последние 10 минут.
SELECT sec_code, board, count() AS rows, max(time) AS last_time
FROM transaq_trades
WHERE time > now() - INTERVAL 10 MINUTE
GROUP BY sec_code, board
ORDER BY sec_code, board;

-- Свечи: история и минутная агрегация quotations лежат вместе.
SELECT sec_code, period, count() AS rows, max(date) AS last_date
FROM transaq_candles
WHERE date > now() - INTERVAL 1 DAY
GROUP BY sec_code, period
ORDER BY sec_code, period;

-- При текущем exporter пусто ожидаемо: подписка quotes не отправляется.
SELECT sec_code, board, count() AS rows, max(time) AS received_at
FROM transaq_quotes
WHERE time > now() - INTERVAL 10 MINUTE
GROUP BY sec_code, board
ORDER BY sec_code, board;

-- Размеры снимочных справочников; событийного времени у них нет.
SELECT 'securities' AS catalog, count() AS rows FROM transaq_securities
UNION ALL
SELECT 'securities_info', count() FROM transaq_securities_info;

-- Дубликаты сделок по полному ключу сортировки, возможные до merge.
SELECT secid, board, sec_code, trade_no, time, buy_sell, count() AS n
FROM transaq_trades
GROUP BY secid, board, sec_code, trade_no, time, buy_sell
HAVING n > 1
LIMIT 20;

-- Одна видимая версия инструмента в снимочном каталоге.
SELECT secid, seccode, board, market, shortname, minstep, lotsize
FROM transaq_securities
FINAL
WHERE seccode = 'SBER';

-- Проверка единиц: quantity в лотах, lotsize из каталога.
SELECT t.sec_code, t.board,
       sum(t.quantity) AS lots,
       sum(t.quantity * toFloat64(s.lotsize)) AS estimated_units
FROM transaq_trades AS t
LEFT JOIN transaq_securities AS s
  ON t.sec_code = s.seccode AND t.board = s.board
WHERE t.time > now() - INTERVAL 1 DAY
GROUP BY t.sec_code, t.board
ORDER BY t.sec_code, t.board;
```

Последний запрос — только оценка: при дублях каталога/сделок, смене лота или
неоднозначном `board` его нужно дополнить дедупликацией и актуальным as-of
соединением. Для FORTS денежный объём также нельзя получать универсальным
умножением на `lotsize` без учёта спецификации контракта.

В журнале экспортёра проверяйте:

```bash
journalctl -u transaq-clickhouse-exporter --since '10 min ago' \
  | grep -E 'subscriptions restored|queue reached|session stopped|ClickHouse|ERROR'
```

Искомые признаки:

- `TRANSAQ subscriptions restored` — подписки восстановлены после подключения;
- отсутствие повторяющихся `session stopped` — нет reconnect-шторма;
- отсутствие роста `queue reached` — ClickHouse успевает за потоком;
- строки в `transaq_trades` и/или `transaq_candles` со свежим временем —
  подтверждение реальной доставки, а не только успешной команды.

## 8. Правила изменений

1. **XML-структура — shared-контракт.** При изменении `commands.Trade`,
   `Quotation`, `Quote`, `Candles`, `Security`, `SecInfo`, `Positions` или
   `UnitedPortfolio` обновите одновременно декодирование `txmlconnector`,
   обработчики экспортёра, тесты XML и этот документ.
2. **DDL и INSERT меняются вместе.** Новая колонка требует согласованного
   изменения `CREATE TABLE`, порядка `batch.Append`/`AsyncInsert`, существующих
   установок через миграцию и разделов §2/§4. `CREATE TABLE IF NOT EXISTS` сам
   не добавляет колонки в уже созданную таблицу.
3. **Ключ `ReplacingMergeTree` — семантика данных.** Изменение `ORDER BY`
   меняет, какие строки будут считаться версиями одной записи. Для журнала
   событий не выбирайте ключ состояния только ради удобства текущего снимка.
4. **Единицы фиксируются на входе.** Лоты, бумаги, рубли и млн руб. нельзя
   смешивать. Конверсия должна ссылаться на актуальный `lotsize`, `lotdivider`,
   `point_cost`, `pname`/`currencyid` и тип инструмента.
5. **Отсутствие поля не превращать молча в ноль.** Если потребителю важно
   различать missing/zero, числовые поля и схему нужно переводить на
   указатели/`Nullable` с совместимой миграцией.
6. **Время должно иметь происхождение.** Биржевое время сделки, время приёма
   quotes и время записи — разные значения. Новые потоки должны явно разделять
   `event_time` и `ingested_at`, а при необходимости использовать
   `DateTime64` вместо секундного `DateTime`.
7. **Подписка и таблица проверяются отдельно.** Добавление handler без
   `Command.Quotes`/другой подписки не наполняет таблицу. Для каждого нового
   потока указывайте команду подключения, reconnect-поведение и SQL-проверку.
8. **OFI/CVD/L2 — отдельные производные.** Их внедрение требует правил сборки
   книги, дедупликации `trade_no`, сессионности накопителей и восстановления
   после разрыва; недостаточно переименовать существующие таблицы.
9. **Секреты не входят в схему.** Примеры конфигурации содержат только имена
   переменных и несекретные значения; логины, пароли и реальные идентификаторы
   клиентов не документируются.
