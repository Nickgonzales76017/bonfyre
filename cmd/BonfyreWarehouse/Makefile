CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
LDLIBS = -lsqlite3
TARGET = bonfyre-warehouse
SRC = src/main.c

.PHONY: all test clean
all: $(TARGET)
$(TARGET): $(SRC) src/cohort_audit.h src/warehouse_verify.h
	$(CC) $(CFLAGS) -o $@ $< $(LDLIBS)

test: $(TARGET)
	$(MAKE) -C ../BonfyreWarehouseSeed all
	bun test ../../scripts/tests/warehouse_catalog_audit.test.mjs ../../scripts/tests/representation_manifold_warehouse_head.test.mjs tests/native_warehouse.test.mjs
	bun test ../../integrations/mcp/tests/project_native.test.mjs

clean:
	rm -f $(TARGET)
