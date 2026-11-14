CC      ?= cc
CFLAGS  ?= -Os -flto -ffunction-sections -fdata-sections -Wall -Wextra -std=c11
ifeq ($(shell uname -s),Darwin)
# Isolated acceptance fixtures use mkdtemp under the POSIX feature profile.
CFLAGS += -D_DARWIN_C_SOURCE
endif
LOCAL_CFLAGS = -I../../engine/core/include -I../../lib/libbonfyre/include
LDFLAGS ?= ../../engine/core/libbonfyre-fabric.a -lsqlite3
TARGET := bonfyre-queue
ifeq ($(shell uname -s),Darwin)
LDFLAGS += -Wl,-dead_strip
else
LDFLAGS += -Wl,--gc-sections
endif

# Compilation never launches browser fixtures, account tests or the ratchet.
# Test owners are explicit: fast native/dispatch smoke, then full acceptance.
.PHONY: all smoke test
all: bonfyre-queue

smoke: bonfyre-queue
	@bash ../../scripts/verification/test-gate.sh run --suite queue_vectors
	$(MAKE) test-account-activity
	$(MAKE) test-account-inbox
	$(MAKE) test-command-dispatch

# Serialize suites: Chrome and native account fixtures must not run
# concurrently even when the caller uses make -j.
test: smoke
	$(MAKE) test-account-wire
	$(MAKE) test-account-work-index

# Source-size governance is enforced by pre-commit, CI and release ownership,
# not by a single account-work integration suite.
.PHONY: test-governance test-release
test-governance:
	sh ../../scripts/native-ratchet.sh check
test-release: test
	$(MAKE) test-governance
	$(MAKE) test-alias-shadow

.PHONY: test-alias-shadow
test-alias-shadow:
	sh tests/installed_alias_shadow_test.sh

MANAGED_PREFIX ?= $(HOME)/.bonfyre
install: all
	install -d "$(MANAGED_PREFIX)/bin"
	install -m 755 bonfyre-queue "$(MANAGED_PREFIX)/bin/.bonfyre-queue.next"
	mv -f "$(MANAGED_PREFIX)/bin/.bonfyre-queue.next" "$(MANAGED_PREFIX)/bin/bonfyre-queue"

# Keep each original Bun process boundary, but expose only bounded results.
# Failed logs remain private on disk; successful logs are deleted.
define quiet_bun
	@set -eu; umask 077; log=$$(mktemp "$${TMPDIR:-/tmp}/bonfyre-queue-bun.XXXXXX"); \
	if $(1) >"$$log" 2>&1; then \
	  result=$$(grep -E '^[[:space:]]*[1-9][0-9]* pass' "$$log" | tail -n 1); \
	  if [ -z "$$result" ]; then echo "REFUSE $(2): no positive test evidence; $$log" >&2; exit 1; fi; \
	  printf 'PASS %s (%s)\n' '$(2)' "$$result"; rm -f "$$log"; \
	else echo "FAIL $(2): $$log" >&2; tail -c 6000 "$$log" >&2; exit 1; fi
endef

.PHONY: test-account-wire test-account-work-index test-command-dispatch
test-command-dispatch: bonfyre-queue
	sh tests/dispatch_test.sh ./bonfyre-queue
	$(call quiet_bun,bun test --reporter=dots ../../scripts/tests/native_command_tables.test.mjs --test-name-pattern "QUEUE_COMMANDS|WEB_ADDRESS_COMMANDS|atlas registers",native-command-tables)

test-account-work-index: bonfyre-queue
	sh tests/account_work_index_test.sh ./bonfyre-queue

test-account-wire: bonfyre-queue
	$(call quiet_bun,BONFYRE_TEST_QUEUE="$(CURDIR)/bonfyre-queue" bun test --reporter=dots ../../integrations/browser/tests/activity-source.test.mjs,account-activity-source)
	$(call quiet_bun,bun test --reporter=dots ../../integrations/browser/tests/aurekai-activity-interaction.browser.test.mjs --test-name-pattern "account work usage is visible",account-work-browser)
	$(call quiet_bun,bun test --reporter=dots ../../integrations/mcp/tests/hook_bridge.test.mjs ../../integrations/mcp/tests/fabric_transport.test.mjs ../../integrations/mcp/tests/fabric_store.test.mjs,fabric-bridge)
	$(call quiet_bun,bun test --reporter=dots ../../integrations/mcp/tests/workspace_recovery_mcp.test.mjs,workspace-recovery)
	sh tests/historical_hook_audit.sh
	@if [ -f "$(HOME)/.bonfyre/state/account-work.db" ]; then ./bonfyre-queue account-work audit --db "$(HOME)/.bonfyre/state/account-work.db" --subject "$${BONFYRE_TEST_ACCOUNT_SUBJECT:-user:19}" --scope self; else echo "account-work audit: SKIP (no local owner index)"; fi
	sh tests/live_account_shadow_test.sh ./bonfyre-queue
	sh tests/installed_footprint_witness.sh

bonfyre-queue: Makefile src/main.c src/account_activity.h src/account_inbox.h src/account_work.h ../../lib/libbonfyre/include/bf_command_lookup.h ../../lib/libbonfyre/include/bf_msgpack.h ../../lib/libbonfyre/bf_msgpack.c ../../engine/core/libbonfyre-fabric.a
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o bonfyre-queue src/main.c ../../lib/libbonfyre/bf_msgpack.c $(LDFLAGS)

account-activity-test: tests/account_activity_test.c src/account_activity.h ../../engine/core/libbonfyre-fabric.a
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $@ tests/account_activity_test.c ../../lib/libbonfyre/bf_msgpack.c $(LDFLAGS)

.PHONY: test-account-activity
test-account-activity: account-activity-test
	./account-activity-test

account-inbox-test: tests/account_inbox_test.c src/account_inbox.h src/account_activity.h src/account_work.h ../../engine/core/libbonfyre-fabric.a
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $@ tests/account_inbox_test.c ../../lib/libbonfyre/bf_msgpack.c $(LDFLAGS)

.PHONY: test-account-inbox
test-account-inbox: account-inbox-test
	./account-inbox-test

../../engine/core/libbonfyre-fabric.a:
	$(MAKE) -C ../../engine/core

clean:
	rm -f bonfyre-queue account-activity-test account-inbox-test
