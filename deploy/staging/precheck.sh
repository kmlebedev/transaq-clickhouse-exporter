#!/bin/bash
# Проверка хоста перед деплоем экспортёра. Выполняется через `ssh palmshell 'bash -s'`,
# значения секретов не печатаются.
set -euo pipefail

SECRETS="${TCE_SECRETS:-/etc/transaq-clickhouse-exporter.env}"
EXPECTED_KEYS=(TC_LOGIN TC_PASSWORD CLICKHOUSE_URL)

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

[ "$(id -u)" = 0 ] || fail "нужен root на $(hostname)"
command -v systemctl >/dev/null || fail "нет systemctl"
command -v clickhouse-client >/dev/null || fail "нет clickhouse-client"

systemctl is-active --quiet txmlconnector-server.service \
    || fail "txmlconnector-server.service не active: экспортёру некуда подключаться"
ss -ltn | grep -q ':50051 ' \
    || fail "порт 50051 не слушается: gRPC-сервер txmlconnector не поднят"

[ -f "$SECRETS" ] || fail "нет $SECRETS — создаёт владелец (см. deploy/staging/README.md)"
mode=$(stat -c '%a' "$SECRETS")
[ "$mode" = "600" ] || fail "у $SECRETS права $mode, нужны 600"

# systemd EnvironmentFile читает только VAR=value: строку «export VAR=value» он
# пропускает с warning, и переменная в процесс не попадает.
if grep -qE '^[[:space:]]*export[[:space:]]+[A-Za-z_][A-Za-z0-9_]*=' "$SECRETS"; then
    fail "$SECRETS записан в shell-форме (export VAR=...): уберите префикс export — sed -i 's/^export //' $SECRETS"
fi

for key in "${EXPECTED_KEYS[@]}"; do
    value=$(grep -E "^${key}=" "$SECRETS" | head -1 | cut -d= -f2- | tr -d '"' || true)
    [ -n "$value" ] || fail "в $SECRETS пустой или отсутствует $key"
done

dsn=$(grep -E '^CLICKHOUSE_URL=' "$SECRETS" | head -1 | cut -d= -f2- | tr -d '"')
case "$dsn" in
    clickhouse://*) ;;
    *) fail "CLICKHOUSE_URL должен быть DSN вида clickhouse://user:password@host:9000/db" ;;
esac
user_part=${dsn#*://}
ch_user=${user_part%%:*}
rest=${user_part#*:}
ch_pass=${rest%%@*}
[ "$ch_user" != "$user_part" ] || fail "в CLICKHOUSE_URL нет пользователя"
[ "$ch_pass" != "$rest" ] || fail "в CLICKHOUSE_URL нет пароля"

clickhouse-client --user "$ch_user" --password "$ch_pass" --query 'SELECT 1' >/dev/null \
    || fail "ClickHouse не принял CLICKHOUSE_URL из $SECRETS"

echo "OK   $(hostname): root, txmlconnector active, :50051, $SECRETS (600), ClickHouse отвечает"
