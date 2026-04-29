# BonfyreDetectObjects — Object detection operator

CC      := cc
FRAGMENT_INC := ../../lib/libfragment/include
FRAGMENT_LIB := ../../lib/libfragment
OSSL_INC     := /opt/homebrew/opt/openssl@3/include
OSSL_LIB     := /opt/homebrew/opt/openssl@3/lib
SQLITE_LIB   := /opt/homebrew/opt/sqlite/lib
CFLAGS  := -O2 -Wall -Wextra -std=c11 \
           -I$(FRAGMENT_INC) -I$(OSSL_INC)
LDFLAGS := -L$(FRAGMENT_LIB) -lfragment \
           -L$(OSSL_LIB) -lssl -lcrypto \
           -L$(SQLITE_LIB) -lsqlite3

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
