CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
CFLAGS += -I../../lib/libbonfyre/include -I/opt/homebrew/include
LDLIBS += ../../lib/libbonfyre/libbonfyre.a -L/opt/homebrew/lib -lduckdb -lm

all: bonfyre-connection-catalog

bonfyre-connection-catalog: src/main.c ../../lib/libbonfyre/libbonfyre.a
	$(CC) $(CFLAGS) -o $@ $< $(LDLIBS)

test: bonfyre-connection-catalog
	sh tests/catalog_test.sh ./bonfyre-connection-catalog
	bun test ../../scripts/tests/connections-rollout-freeze.test.mjs

.PHONY: all test
