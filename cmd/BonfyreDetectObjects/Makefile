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
VISION := bonfyre-detect-vision

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

install-vision: $(BUILD)/$(VISION)
	mkdir -p ~/bin
	cp $(BUILD)/$(VISION) ~/bin/

# The detector this machine carries. It is built where the system's detection
# framework exists; elsewhere BONFYRE_YOLO_BINARY names the detector.
$(BUILD)/$(VISION): src/vision_detect.swift | $(BUILD)
	swiftc -O -o $@ $<

# TEST_IMAGE is a real photograph with a person or a face in it.
test: $(BUILD)/$(BIN) $(BUILD)/$(VISION)
	@test -n "$(TEST_IMAGE)" || { echo "set TEST_IMAGE to a real photograph of a person"; exit 1; }
	sh tests/detect_test.sh $(BUILD)/$(BIN) $(BUILD)/$(VISION) "$(TEST_IMAGE)"
