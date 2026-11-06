CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11 -ffunction-sections -fdata-sections
LOCAL_CFLAGS = -I../../lib/libbonfyre/include -I../BonfyreLex/src
LDFLAGS = ../../lib/libbonfyre/libbonfyre.a -lsqlite3
TARGET = bonfyre-discipl
SRC = src/main.c
# The binary links this archive, so it has to depend on it. Without the
# prerequisite a rebuilt library leaves a stale binary behind and `make`
# reports success -- the change is compiled, installed and not running.
LIB = ../../lib/libbonfyre/libbonfyre.a
SYNC_BIN = $(HOME)/.bonfyre/bin/$(TARGET)

all: $(TARGET)

$(TARGET): $(SRC) $(LIB) ../../lib/libbonfyre/include/bf_command_lookup.h
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -Wl,-dead_strip -o $(TARGET) $(SRC) $(LDFLAGS)

install-bin: $(TARGET)
	@mkdir -p $(dir $(SYNC_BIN))
	@cp $(TARGET) $(SYNC_BIN)
	@codesign --force --sign - $(SYNC_BIN) >/dev/null 2>&1 || true

clean:
	rm -f $(TARGET)

.PHONY: all clean install-bin
