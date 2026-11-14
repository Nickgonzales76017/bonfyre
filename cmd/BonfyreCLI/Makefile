CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
CFLAGS += -I../BonfyreLex/src
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LDFLAGS = ../../lib/libbonfyre/libbonfyre.a -lsqlite3
TARGET = bonfyre
SRC = src/main.c
PREFIX ?= $(HOME)/.local
BINDIR ?= $(PREFIX)/bin

all: $(TARGET) contract-test

contract-test: $(TARGET)
	@set -e; log=$(mktemp); trap 'rm -f "$log"' EXIT; \
	if bun tests/public_command_contract.mjs >"$log" 2>&1; then grep '^public command contract: PASS' "$log"; else cat "$log" >&2; exit 1; fi
	@bun test --reporter=dots tests/front_door.test.mjs tests/help_contract.test.mjs

.PHONY: test verification-contract
test: verification-contract
verification-contract:
	@bash ../../scripts/tests/test-gate.test.sh

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

clean:
	rm -f $(TARGET)

install: $(TARGET)
	install -d $(DESTDIR)$(BINDIR)
	install -m 755 $(TARGET) $(DESTDIR)$(BINDIR)/$(TARGET)

.PHONY: all clean install
