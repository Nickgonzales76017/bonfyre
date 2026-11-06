CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
CFLAGS += -I../../engine/core/include -I../../lib/libbonfyre/include
LDFLAGS ?= ../../engine/core/libbonfyre-fabric.a -lsqlite3
TARGET = bonfyre-provider-frontier

.PHONY: all clean test

all: $(TARGET)

$(TARGET): src/main.c ../../engine/core/libbonfyre-fabric.a
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

../../engine/core/libbonfyre-fabric.a:
	$(MAKE) -C ../../engine/core

test: $(TARGET)
	bash tests/provider_frontier_test.sh ./$(TARGET)
	bun test tests/refresh_providers.test.mjs

clean:
	rm -f $(TARGET)
