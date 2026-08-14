# Transaq Clickhouse Exporter

![img_2.png](img_2.png)
![img_1.png](img_1.png)

## Description

gRPC клиент от [txmlconnector`а](https://github.com/kmlebedev/txmlconnector) для экспорта данных торгов ММВБ в базу данных [ClickHouse](https://clickhouse.com/)

## Переподключение

Переподключением управляет `txmlconnector`:

- exporter завершает текущую сессию при статусе терминала `connected=false`/`connected=error`, закрытии gRPC response stream или ошибке восстановления подписок;
- `txmlconnector.RunWithReconnect` закрывает старый `TCClient` и создаёт новый с экспоненциальной задержкой от 1 до 30 секунд;
- после статуса `connected=true` exporter заново формирует и отправляет подписки без накопления дубликатов.

Параметры переподключения централизованы в `txmlconnector.DefaultReconnectConfig`; отдельная переменная `TC_RECONNECT_INTERVAL` exporter больше не используется.

Входящие сделки, котировки и обновления инструментов дренируются независимо от восстановления подписок и записи в ClickHouse. Сделки и котировки записываются пакетами. Если ClickHouse длительно не успевает обрабатывать поток, экспортёр выводит предупреждение `TRANSAQ ... queue reached ... events`; это означает, что нужно проверить задержки и доступность ClickHouse.
