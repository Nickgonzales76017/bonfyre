# BonfyreDetectObjects — Object detection operator

CC      := cc
FRAGMENT_INC := ../../lib/libfragment/include
FRAGMENT_LIB := ../../lib/libfragment
OPENSSL_CFLAGS := $(shell pkg-config --cflags openssl  2>/dev/null || echo "-I/opt/homebrew/opt/openssl@3/include")
OPENSSL_LIBS   := $(shell pkg-config --libs   openssl  2>/dev/null || echo "-L/opt/homebrew/opt/openssl@3/lib -lssl -lcrypto")
SQLITE_LIBS    := $(shell pkg-config --libs   sqlite3  2>/dev/null || echo "-L/opt/homebrew/opt/sqlite/lib -lsqlite3")
CFLAGS  := -O2 -Wall -Wextra -std=c11 \
           -I$(FRAGMENT_INC) $(OPENSSL_CFLAGS)
LDFLAGS := -L$(FRAGMENT_LIB) -lfragment \
           $(OPENSSL_LIBS) \
           $(SQLITE_LIBS)

SRC    := src/main.c
BUILD  := build
BIN    := bonfyre-detect-objects

.PHONY: all clean install test

all: $(BUILD)/$(BIN)

$(BUILD)/$(BIN): $(SRC) $(FRAGMENT_LIB)/libfragment.a | $(BUILD)
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

$(FRAGMENT_LIB)/libfragment.a:
	$(MAKE) -C $(FRAGMENT_LIB)

$(BUILD):
	mkdir -p $(BUILD)

clean:
	rm -rf $(BUILD)

install: $(BUILD)/$(BIN)
	mkdir -p ~/bin
	cp $(BUILD)/$(BIN) ~/bin/
	@echo "Installed to ~/bin/$(BIN)"

test: $(BUILD)/$(BIN)
	@echo "=== BonfyreDetectObjects smoke tests ==="
	@# Single image (stub path)
	@mkdir -p /tmp/bf-detect-test/single
	@echo "testing single JPEG stub..."
	@echo "" > /tmp/test-frame.jpg
	@./$(BUILD)/$(BIN) --input /tmp/test-frame.jpg --out /tmp/bf-detect-test/single
	@test -f /tmp/bf-detect-test/single/objects.json && echo "✓ objects.json created" || echo "✗ objects.json missing"
	@# Directory path
	@mkdir -p /tmp/bf-detect-test/frames
	@echo "" > /tmp/bf-detect-test/frames/frame_001.jpg
	@echo "" > /tmp/bf-detect-test/frames/frame_002.jpg
	@./$(BUILD)/$(BIN) --input /tmp/bf-detect-test/frames --out /tmp/bf-detect-test/out
	@test -f /tmp/bf-detect-test/out/objects.json && echo "✓ merged objects.json created" || echo "✗ merged missing"
	@echo "=== Tests done ==="
