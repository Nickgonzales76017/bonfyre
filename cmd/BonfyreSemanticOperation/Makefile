CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
ROOT := ../..
TARGET := bonfyre-semantic-operation
.PHONY: all test clean
all: $(TARGET) test
$(TARGET): src/main.c $(ROOT)/engine/core/src/target_obligation.c ../BonfyreSemanticOptimizer/generated/inverse_programs.h
	$(CC) $(CFLAGS) -I$(ROOT)/engine/core/include -I../BonfyreSemanticOptimizer/generated -o $@ src/main.c $(ROOT)/engine/core/src/target_obligation.c -lsqlite3
test: $(TARGET)
	@tmp=$$(mktemp -d); trap 'rm -rf "$$tmp"' EXIT; sqlite3 "$$tmp/cms.db" "CREATE TABLE proposal_contract(id INTEGER PRIMARY KEY,operation TEXT,semantic_contract TEXT,preference_policy TEXT,predicate_id TEXT,owner TEXT,predicate TEXT,expected_state TEXT,evidence_requirement TEXT,authority_requirement TEXT,valid_start TEXT,valid_end TEXT,status TEXT); INSERT INTO proposal_contract VALUES(1,'Budget.request','budget-funded','frugal','p:settled','CommerceChain','payment','settled','','','','','published');"; ./$(TARGET) propose-contract --cms-db "$$tmp/cms.db" --db "$$tmp/e.db" --operation Budget.request --target-id target:budget --target budget:fixture --subject commerce:fixture --frontier proof:budget --owner MadeUp >"$$tmp/r"; grep -q '"operation":"Budget.request"' "$$tmp/r"; grep -q '"owner":"CommerceChain"' "$$tmp/r"; grep -q '"native_state":27' "$$tmp/r"; grep -q '"owner_query":10' "$$tmp/r"; ! grep -q MadeUp "$$tmp/r"
	@tmp=$$(mktemp -d); trap 'rm -rf "$$tmp"' EXIT; ./$(TARGET) propose --db "$$tmp/e.db" --target-id target:fixture --target world:fixture --subject foreign:fixture --contract observed --frontier proof:fixture --policy resilient --predicate-id p:observed --owner ForeignTwin --predicate delivery --expected observed >"$$tmp/r"; grep -q '"state":"OPEN"' "$$tmp/r"; grep -q '"adapter":8' "$$tmp/r"; grep -q '"native_state":14' "$$tmp/r"; grep -q '"owner_query":1' "$$tmp/r"; ! ./$(TARGET) propose --db "$$tmp/e.db" --target-id bad --target x --subject x --contract x --frontier x --policy resilient --predicate-id p --owner MadeUp --predicate delivery --expected observed >/dev/null 2>&1
clean:
	rm -f $(TARGET)
