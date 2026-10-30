# transaq-clickhouse-exporter: сборка и деплой на palmshell (см. deploy/staging/README.md).
SHELL := /bin/bash

BIN              ?= build/transaq-clickhouse-exporter
UNIVERSE         ?= deploy/staging/universe-imoex-ofz.txt
STAGING_HOST     ?= palmshell
STAGING_SSH      ?= ssh
STAGING_SCP      ?= scp
STAGING_BIN_DIR  ?= /root/bin
STAGING_SERVICE  ?= transaq-clickhouse-exporter.service
STAGING_UNIT_PATH  ?= /etc/systemd/system/$(STAGING_SERVICE)
STAGING_SECRETS    ?= /etc/transaq-clickhouse-exporter.env
STAGING_EXPORT_ENV ?= /etc/transaq-clickhouse-exporter.export.env
STAGING_REMOTE_BIN ?= $(STAGING_BIN_DIR)/transaq-clickhouse-exporter
STAGING_UNIT_TMPL  ?= deploy/staging/transaq-clickhouse-exporter.service
STAGING_ENV_TMPL   ?= deploy/staging/exporter.export.env.in
STAGING_PRECHECK   ?= deploy/staging/precheck.sh
STAGING_CHECK      ?= deploy/staging/check.sh

.PHONY: all build test vet universe-refresh universe-check staging-precheck \
	staging-prepare deploy-staging deploy-staging-binary deploy-staging-env \
	deploy-staging-unit deploy-staging-smoke deploy-staging-check \
	deploy-staging-status deploy-staging-logs deploy-staging-restart rollback-staging

all: test vet build

# GOWORK=off: локальный go.work подключает соседнее дерево ../txmlconnector,
# а на стенд должен уходить прибитый в go.mod модуль.
build:
	@mkdir -p build
	GOWORK=off GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "-s -w" -o $(BIN) .

test:
	GOWORK=off go test -race ./...

vet:
	GOWORK=off go vet ./...

# Вселенная подписки: акции IMOEX + ОФЗ TQOB. Обновлять перед ребалансировкой
# (третья пятница марта/июня/сентября/декабря) и на каждый новый выпуск ОФЗ.
universe-refresh:
	GOWORK=off go run ./cmd/universe -write -out $(UNIVERSE)

universe-check:
	GOWORK=off go run ./cmd/universe -check -out $(UNIVERSE)

staging-precheck:
	@$(STAGING_SSH) $(STAGING_HOST) 'bash -s' < $(STAGING_PRECHECK)

staging-prepare:
	@$(STAGING_SSH) $(STAGING_HOST) 'install -d -m 755 $(STAGING_BIN_DIR)'

deploy-staging-binary: staging-prepare build
	@set -e; \
	local_sum=$$(shasum -a 256 "$(BIN)" | cut -d' ' -f1); \
	remote_sum=$$($(STAGING_SSH) $(STAGING_HOST) 'sha256sum "$(STAGING_REMOTE_BIN)" 2>/dev/null | cut -d" " -f1' || true); \
	if [ "$$local_sum" = "$$remote_sum" ]; then \
		echo "OK   бинарь не изменился ($$local_sum)"; \
	else \
		$(STAGING_SSH) $(STAGING_HOST) 'if [ -f "$(STAGING_REMOTE_BIN)" ]; then cp -f "$(STAGING_REMOTE_BIN)" "$(STAGING_REMOTE_BIN).bak"; fi'; \
		$(STAGING_SCP) "$(BIN)" "$(STAGING_HOST):$(STAGING_REMOTE_BIN).new"; \
		$(STAGING_SSH) $(STAGING_HOST) 'mv "$(STAGING_REMOTE_BIN).new" "$(STAGING_REMOTE_BIN)" && chmod 755 "$(STAGING_REMOTE_BIN)"'; \
		echo "OK   бинарь обновлён"; \
	fi

deploy-staging-env:
	@set -e; \
	test -s "$(UNIVERSE)" || { echo "FAIL: пустой $(UNIVERSE)"; exit 1; }; \
	count=$$(grep -c . "$(UNIVERSE)"); \
	if [ "$$count" -lt 20 ]; then echo "FAIL: вселенная из $$count кодов — похоже на сбой источника"; exit 1; fi; \
	codes=$$(paste -sd, "$(UNIVERSE)"); \
	tmp=$$(mktemp); trap 'rm -f "$$tmp"' EXIT; \
	sed -e "s|__EXPORT_ALL_TRADES__|$$codes|g" "$(STAGING_ENV_TMPL)" > "$$tmp"; \
	if grep -q '__[A-Z_]*__' "$$tmp"; then echo "FAIL: в env остались плейсхолдеры"; exit 1; fi; \
	local_sum=$$(shasum -a 256 "$$tmp" | cut -d' ' -f1); \
	remote_sum=$$($(STAGING_SSH) $(STAGING_HOST) 'sha256sum "$(STAGING_EXPORT_ENV)" 2>/dev/null | cut -d" " -f1' || true); \
	if [ "$$local_sum" = "$$remote_sum" ]; then \
		echo "OK   фильтры экспортёра актуальны ($$count кодов)"; \
	else \
		$(STAGING_SCP) "$$tmp" "$(STAGING_HOST):$(STAGING_EXPORT_ENV).new"; \
		$(STAGING_SSH) $(STAGING_HOST) 'mv "$(STAGING_EXPORT_ENV).new" "$(STAGING_EXPORT_ENV)" && chmod 600 "$(STAGING_EXPORT_ENV)"'; \
		echo "OK   фильтры экспортёра обновлены ($$count кодов)"; \
	fi

deploy-staging-unit:
	@set -e; \
	tmp=$$(mktemp); trap 'rm -f "$$tmp"' EXIT; \
	sed -e 's|__TCE_BIN_DIR__|$(STAGING_BIN_DIR)|g' \
	    -e 's|__TCE_SECRETS__|$(STAGING_SECRETS)|g' \
	    -e 's|__TCE_EXPORT_ENV__|$(STAGING_EXPORT_ENV)|g' \
	    "$(STAGING_UNIT_TMPL)" > "$$tmp"; \
	$(STAGING_SCP) "$$tmp" "$(STAGING_HOST):$(STAGING_UNIT_PATH).new"; \
	$(STAGING_SSH) $(STAGING_HOST) 'mv "$(STAGING_UNIT_PATH).new" "$(STAGING_UNIT_PATH)" && chmod 644 "$(STAGING_UNIT_PATH)" && systemctl daemon-reload'; \
	echo "OK   unit установлен: $(STAGING_UNIT_PATH)"

deploy-staging: staging-precheck deploy-staging-binary deploy-staging-env deploy-staging-unit
	@$(STAGING_SSH) $(STAGING_HOST) 'systemctl enable "$(STAGING_SERVICE)" && systemctl restart "$(STAGING_SERVICE)"'
	@$(MAKE) deploy-staging-smoke --no-print-directory

# Подписка восстанавливается не мгновенно: сначала connect, затем каталог
# securities и только потом subscribe. Ждём строку в журнале до 3 минут.
deploy-staging-smoke:
	@$(STAGING_SSH) $(STAGING_HOST) 'systemctl is-active --quiet "$(STAGING_SERVICE)"' || { \
		echo "FAIL: $(STAGING_SERVICE) не active"; \
		$(STAGING_SSH) $(STAGING_HOST) 'journalctl -u "$(STAGING_SERVICE)" -n 50 --no-pager'; exit 1; }
	@ok=0; for i in $$(seq 1 90); do \
		if $(STAGING_SSH) $(STAGING_HOST) 'since=$$(systemctl show -p ActiveEnterTimestamp --value "$(STAGING_SERVICE)"); journalctl -u "$(STAGING_SERVICE)" --since "$$since" --no-pager | grep -q "TRANSAQ subscriptions restored"'; then ok=1; break; fi; \
		sleep 2; \
	done; \
	if [ "$$ok" != "1" ]; then \
		echo "FAIL: в журнале нет «TRANSAQ subscriptions restored» — проверьте TC_LOGIN/TC_PASSWORD и подписку"; \
		$(STAGING_SSH) $(STAGING_HOST) 'journalctl -u "$(STAGING_SERVICE)" -n 50 --no-pager'; exit 1; \
	fi; \
	echo "OK   $(STAGING_SERVICE): active, подписки восстановлены"

# Живое состояние сбора: счётчики, свежесть и покрытие вселенной.
deploy-staging-check:
	@mkdir -p build
	@$(STAGING_SSH) $(STAGING_HOST) 'bash -s' < $(STAGING_CHECK) > build/staging-seccodes.txt
	@missing=$$(comm -23 <(grep -v '^#' "$(UNIVERSE)" | sort) <(sort build/staging-seccodes.txt)); \
	if [ -n "$$missing" ]; then \
		echo "WARN: нет в transaq_securities (подписка молча пропустит):"; echo "$$missing"; \
	else \
		echo "OK   все коды вселенной есть в transaq_securities"; \
	fi

deploy-staging-status:
	@$(STAGING_SSH) $(STAGING_HOST) 'systemctl status "$(STAGING_SERVICE)" --no-pager -l | head -25'

deploy-staging-logs:
	@$(STAGING_SSH) $(STAGING_HOST) 'journalctl -u "$(STAGING_SERVICE)" -n 120 --no-pager'

deploy-staging-restart:
	@$(STAGING_SSH) $(STAGING_HOST) 'systemctl restart "$(STAGING_SERVICE)"'
	@$(MAKE) deploy-staging-smoke --no-print-directory

rollback-staging:
	@$(STAGING_SSH) $(STAGING_HOST) 'test -f "$(STAGING_REMOTE_BIN).bak"' || { echo "FAIL: нет $(STAGING_REMOTE_BIN).bak"; exit 1; }
	@$(STAGING_SSH) $(STAGING_HOST) 'cp -f "$(STAGING_REMOTE_BIN).bak" "$(STAGING_REMOTE_BIN)" && chmod 755 "$(STAGING_REMOTE_BIN)" && systemctl restart "$(STAGING_SERVICE)"'
	@$(MAKE) deploy-staging-smoke --no-print-directory
