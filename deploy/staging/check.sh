#!/bin/bash
# Состояние сбора на хосте: печатает в stderr счётчики и свежесть ленты, в stdout —
# коды из transaq_securities (по одному на строку) для сверки с вселенной.
set -euo pipefail

SECRETS="${TCE_SECRETS:-/etc/transaq-clickhouse-exporter.env}"
EXPORT_ENV="${TCE_EXPORT_ENV:-/etc/transaq-clickhouse-exporter.export.env}"

[ -r "$SECRETS" ] || { echo "FAIL: нет $SECRETS" >&2; exit 1; }
[ -r "$EXPORT_ENV" ] || { echo "FAIL: нет $EXPORT_ENV — сначала make deploy-staging-env" >&2; exit 1; }

dsn=$(grep -E '^CLICKHOUSE_URL=' "$SECRETS" | head -1 | cut -d= -f2- | tr -d '"')
user_part=${dsn#*://}
ch_user=${user_part%%:*}
rest=${user_part#*:}
ch_pass=${rest%%@*}

query() {
    clickhouse-client --user "$ch_user" --password "$ch_pass" --query "$1"
}

securities=$(query "SELECT count() FROM transaq_securities FINAL")
trades=$(query "SELECT count() FROM transaq_trades FINAL")
last_trade=$(query "SELECT ifNull(toString(max(time)), '') FROM transaq_trades")
last_minute=$(query "SELECT ifNull(toString(max(minute)), '') FROM transaq_net_flows" 2>/dev/null || true)
codes=$(grep -E '^EXPORT_ALL_TRADES=' "$EXPORT_ENV" | head -1 | cut -d= -f2- | tr ',' '\n' | grep -c . || true)

echo "transaq_securities: $securities строк" >&2
echo "transaq_trades: $trades строк, последняя сделка: ${last_trade:-нет}" >&2
echo "transaq_net_flows: последняя минута: ${last_minute:-нет}" >&2
echo "вселенная в EXPORT_ALL_TRADES: ${codes:-?} кодов" >&2

query "SELECT DISTINCT trimBoth(toString(seccode)) FROM transaq_securities FINAL ORDER BY 1"
