CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
ROOT := ../..
TARGET := bonfyre-target-attention
.PHONY: all test clean
all: $(TARGET) test
$(TARGET): src/main.c $(ROOT)/engine/core/src/target_obligation.c
	$(CC) $(CFLAGS) -I$(ROOT)/engine/core/include -o $@ src/main.c $(ROOT)/engine/core/src/target_obligation.c -lsqlite3
test: $(TARGET)
	@$(MAKE) -C ../BonfyreAttention all >/dev/null; $(MAKE) -C ../BonfyreSemanticOperation all >/dev/null; tmp=$$(mktemp -d); trap 'rm -rf "$$tmp"' EXIT; ../BonfyreSemanticOperation/bonfyre-semantic-operation propose --db "$$tmp/target.db" --target-id target:attention-fixture --target world:fixture --subject foreign:fixture --contract observed --frontier proof:fixture --policy resilient --predicate-id p:observed --owner ForeignTwin --predicate result --expected observed >/dev/null; ./$(TARGET) sync --target-db "$$tmp/target.db" --attention-db "$$tmp/attention.db" --target target:attention-fixture --attention-bin ../BonfyreAttention/bonfyre-attention >"$$tmp/out"; grep -q '"target_state":"OPEN"' "$$tmp/out"; grep -q '"temperature":"hot"' "$$tmp/out"; test "$$(sqlite3 "$$tmp/attention.db" "SELECT temperature FROM watches WHERE watch_id='target-state:target:attention-fixture';")" = hot
clean:
	rm -f $(TARGET)
