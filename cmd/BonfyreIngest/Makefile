CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LIB = ../../lib/libbonfyre/libbonfyre.a
TARGET = bonfyre-ingest
SRC = src/main.c
SYNC_BIN = $(HOME)/.bonfyre/bin/$(TARGET)

all: $(TARGET)

$(LIB): force-library
	$(MAKE) -C ../../lib/libbonfyre

$(TARGET): $(SRC) $(LIB)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $(TARGET) $(SRC) $(LIB) $(LDFLAGS)

test: $(TARGET)
	bun test tests/hash.test.mjs

sync: $(TARGET)
	mkdir -p $(HOME)/.bonfyre/bin
	cp $(TARGET) $(SYNC_BIN)
	codesign -s - $(SYNC_BIN) >/dev/null 2>&1 || true

clean:
	rm -f $(TARGET)

.PHONY: all sync clean test force-library
