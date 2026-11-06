CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
LDLIBS = -lsqlite3
TARGET = bonfyre-warehouse
SRC = src/main.c

.PHONY: all test clean
all: $(TARGET)
$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $@ $< $(LDLIBS)

test: $(TARGET)
	bun test ../../scripts/tests/warehouse_catalog_audit.test.mjs ../../scripts/tests/representation_manifold_warehouse_head.test.mjs tests/native_warehouse.test.mjs

clean:
	rm -f $(TARGET)
