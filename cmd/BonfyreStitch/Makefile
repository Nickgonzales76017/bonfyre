CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11 -ffunction-sections -fdata-sections
CFLAGS += -I../../cmd/BonfyreLex/src
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LDFLAGS  = ../../lib/libbonfyre/libbonfyre.a -lsqlite3
TARGET = bonfyre-stitch
SRC = src/main.c

all: $(TARGET)

# Antithesis-inspired property and fault witnesses. All run on local fixtures.
.PHONY: antithesis-quick-witness
antithesis-quick-witness:
	bun test ../../integrations/browser/tests/antithesis-reliability.test.mjs
	bun test ../../integrations/browser/tests/antithesis-triage-intake.test.mjs
	bun test ../../scripts/tests/representation_manifold_warehouse_head.test.mjs
	bun test ../../scripts/tests/antithesis-warehouse-differential.test.mjs
	@if [ -x "../BonfyreQueue/bonfyre-queue" ]; then node --test ../../scripts/tests/research-workgraph-native.test.mjs; else echo "WorkGraph checkpoint acceptance unavailable: missing native BonfyreQueue executable"; exit 1; fi
	bun test ../../integrations/browser/tests/account-connections.test.mjs
	# Signed-in deterministic browser mutation: only the filtered test runs.
	bun test ../../scripts/tests/warehouse-aurekai-browser.test.mjs --test-name-pattern "Bombadil-style signed-in|Antithesis reliability lab"
	sh ../../scripts/tests/publish_r2_route_aliases_test.sh
	sh ../../scripts/tests/placement_plane_test.sh
	sh ../BonfyreWebAddress/tools/preview-snapshot.test.sh

test: $(TARGET) antithesis-quick-witness
	sh ./tests/dispatch_test.sh ./$(TARGET)
	sh ../BonfyreWebAddress/tools/preview-snapshot.test.sh
	node --test tests/recovery_audit.test.mjs
	node --test tests/stitch_identity_recovery.test.mjs
	bun test ../../integrations/mcp/tests/fabric_work.test.mjs
	@if [ -d "$(HOME)/.bonfyre/dev-fabric/worktrees/8dba361a863e4d74be1066f9" ]; then \
	  node tools/recovery-audit.mjs --candidate "$(HOME)/.bonfyre/dev-fabric/worktrees/8dba361a863e4d74be1066f9" --target ../.. ; \
	fi

$(TARGET): $(SRC) ../../lib/libbonfyre/include/bf_command_lookup.h ../../lib/libbonfyre/libbonfyre.a
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -Wl,-dead_strip -o $(TARGET) $(SRC) $(LDFLAGS)

clean:
	rm -f $(TARGET)

.PHONY: all clean test
