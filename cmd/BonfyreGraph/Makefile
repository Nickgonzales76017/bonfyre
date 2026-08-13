CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
LDFLAGS ?= ../../lib/libbonfyre/libbonfyre.a -lsqlite3
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
TARGET = bonfyre-graph
SRC = src/main.c

SYNC_BIN = $(HOME)/.bonfyre/bin/$(TARGET)

all: $(TARGET) install-bin

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

install-bin: $(TARGET)
	@mkdir -p $(dir $(SYNC_BIN))
	@cp $(TARGET) $(SYNC_BIN)
	@codesign --force --sign - $(SYNC_BIN) >/dev/null 2>&1 || true

clean:
	rm -f $(TARGET)

.PHONY: all clean install-bin
