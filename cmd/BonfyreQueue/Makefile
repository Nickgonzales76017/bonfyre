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

# Scope and recipient-state invariants are part of the normal native build.
.PHONY: all
all: bonfyre-queue test-account-activity test-account-inbox test-account-wire test-account-work-index test-command-dispatch

# Suites already run under all; this is the uniform entry point.
.PHONY: test
test: all

MANAGED_PREFIX ?= $(HOME)/.bonfyre
install: all
	install -d "$(MANAGED_PREFIX)/bin"
	install -m 755 bonfyre-queue "$(MANAGED_PREFIX)/bin/.bonfyre-queue.next"
	mv -f "$(MANAGED_PREFIX)/bin/.bonfyre-queue.next" "$(MANAGED_PREFIX)/bin/bonfyre-queue"

.PHONY: test-account-wire test-account-work-index test-command-dispatch
test-command-dispatch: bonfyre-queue
	sh tests/dispatch_test.sh ./bonfyre-queue
	bun test ../../scripts/tests/native_command_tables.test.mjs --test-name-pattern "QUEUE_COMMANDS|WEB_ADDRESS_COMMANDS|atlas registers"

test-account-work-index: bonfyre-queue
	sh tests/account_work_index_test.sh ./bonfyre-queue

test-account-wire: bonfyre-queue
	BONFYRE_TEST_QUEUE="$(CURDIR)/bonfyre-queue" \
	bun test ../../integrations/browser/tests/activity-source.test.mjs
	bun test ../../integrations/browser/tests/aurekai-activity-interaction.browser.test.mjs --test-name-pattern "account work usage is visible"
	bun test ../../integrations/mcp/tests/hook_bridge.test.mjs ../../integrations/mcp/tests/fabric_transport.test.mjs ../../integrations/mcp/tests/fabric_store.test.mjs
	bun test ../../integrations/mcp/tests/workspace_recovery_mcp.test.mjs
	sh tests/historical_hook_audit.sh
	@if [ -f "$(HOME)/.bonfyre/state/account-work.db" ]; then ./bonfyre-queue account-work audit --db "$(HOME)/.bonfyre/state/account-work.db" --subject "$${BONFYRE_TEST_ACCOUNT_SUBJECT:-user:19}" --scope self; else echo "account-work audit: SKIP (no local owner index)"; fi
	sh tests/live_account_shadow_test.sh ./bonfyre-queue
	sh tests/installed_footprint_witness.sh
	sh tests/installed_alias_shadow_test.sh
	sh ../../scripts/native-ratchet.sh check

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
