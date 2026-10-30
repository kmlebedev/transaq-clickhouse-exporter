# Деплой `transaq-clickhouse-exporter` на `palmshell`

Экспортёр — обычный linux/amd64-бинарник без CGO: он говорит по gRPC с
`txmlconnector-server` (тот уже развёрнут на хосте под Wine, порт 50051) и пишет
ленту сделок в ClickHouse. Деплой ставит бинарь, systemd-юнит и файл несекретных
фильтров; секреты создаёт владелец один раз и деплой их не трогает.

## Что где лежит

| Путь | Что |
|---|---|
| `/root/bin/transaq-clickhouse-exporter` | бинарник, запускает systemd |
| `/root/bin/transaq-clickhouse-exporter.bak` | предыдущая версия для `rollback-staging` |
| `/etc/systemd/system/transaq-clickhouse-exporter.service` | юнит |
| `/etc/transaq-clickhouse-exporter.env` | **секреты, создаёт владелец** (600), деплой не пишет |
| `/etc/transaq-clickhouse-exporter.export.env` | **фильтры подписки**, ставит `deploy-staging-env` (600) |

## Одноразово: секреты

```bash
ssh palmshell 'install -m 600 /dev/null /etc/transaq-clickhouse-exporter.env'
# затем вписать в файл (значения задаёт владелец, в git и в чат они не попадают):
#   TC_LOGIN=<логин Transaq>
#   TC_PASSWORD=<пароль>
#   CLICKHOUSE_URL=clickhouse://<user>:<password>@127.0.0.1:9000/default
```

`TC_HOST`, `TC_PORT`, `TC_TARGET`, фильтры и `LOG_LEVEL` — не секреты, они
приезжают из `exporter.export.env.in` при деплое.

## Порядок

```bash
make universe-check      # вселенная не устарела? (exit 1, если ISS разошёлся)
make deploy-staging      # precheck → бинарь → env → юнит → restart → smoke
make deploy-staging-check # счётчики, свежесть ленты и покрытие вселенной
make deploy-staging-status
make deploy-staging-logs
make rollback-staging    # вернуть .bak и перезапустить
```

`deploy-staging` падает, если:

- на хосте нет root, `txmlconnector-server.service` не active или `:50051` не слушается;
- нет файла секретов, у него права не `600`, пустой `TC_LOGIN`/`TC_PASSWORD`/`CLICKHOUSE_URL`;
- ClickHouse не принимает `CLICKHOUSE_URL`;
- после рестарта в журнале за 3 минуты нет `TRANSAQ subscriptions restored`.

## Вселенная подписки

`universe-imoex-ofz.txt` — строгий список кодов: 44 бумаги IMOEX + 61 ОФЗ доски
TQOB. Он подставляется в `EXPORT_ALL_TRADES` (экспортёр сравнивает `seccode`
точно), поэтому:

- **обновлять раз в квартал** перед ребалансировкой индекса (третья пятница
  марта, июня, сентября, декабря) и **на каждый новый выпуск ОФЗ**;
- устаревший список не даёт ошибки: пропущенный инструмент просто не
  подписывается и не попадает в ленту. Проверка — `make deploy-staging-check`
  (сверка кодов с `transaq_securities`) и `make universe-check` в CI/руками;
- `make universe-refresh` перезаписывает файл из MOEX ISS и печатает
  добавленные/убывшие коды.

`EXPORT_SEC_CODES` намеренно пуст: котировки и история свечей не подписываются
(L2 вне scope пункта `ofz-market/1.5`). `EXPORT_SEC_INFO_NAMES=ALL` оставлен,
потому что `transaq_securities_info` — вход импортёра `ofz_issue_analytics`
в проекте `clickhouse-import-rosstat`.

## Диагностика

Пустая лента при `success=true` — известный режим отказа: у брокерского аккаунта
не включена подписка на рыночные данные. Критерий живой проверки — строки и
свежесть, а не код ответа команды:

```bash
ssh palmshell "clickhouse-client --query \"SELECT count(), max(time) FROM transaq_trades\""
ssh palmshell 'journalctl -u transaq-clickhouse-exporter -n 100 --no-pager | grep -E "Subscribe|subscriptions|TRANSAQ"'
```

Вне торговой сессии (10:00–18:50 МСК, будни; вечерняя 19:00–23:50) пустая
лента — штатное состояние.

## Сборка

`make build` — `GOWORK=off GOOS=linux GOARCH=amd64 CGO_ENABLED=0`. `GOWORK=off`
обязателен: локальный `go.work` подключает соседнее дерево `../txmlconnector`, а
на стенд должен уходить модуль, прибитый в `go.mod`.
