CC      = cc
CFLAGS  = -std=c11 -O2 -Wall -Wextra -Wpedantic
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LDFLAGS = -L../../lib/libbonfyre -lbonfyre -lsqlite3
TARGET  = bonfyre-flow
SRC     = src/main.c
SYNC_BIN = $(HOME)/.bonfyre/bin/$(TARGET)

all: $(TARGET) install-bin

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $@ $(SRC) $(LDFLAGS)

install-bin: $(TARGET)
	@mkdir -p $(dir $(SYNC_BIN))
	@cp $(TARGET) $(SYNC_BIN)
	@codesign --force --sign - $(SYNC_BIN) >/dev/null 2>&1 || true

clean:
	rm -f $(TARGET)

.PHONY: all clean install-bin
