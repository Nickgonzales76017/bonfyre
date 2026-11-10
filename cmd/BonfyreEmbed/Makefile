comma := ,
CC ?= cc
# -Os with dead stripping keeps the installed organ inside the native ratchet's
# installed_bonfyre_bytes budget after the admission ledger was linked in.
CFLAGS ?= -Os -Wall -Wextra -std=c11 -ffunction-sections -fdata-sections
CFLAGS += -I../BonfyreLex/src
SQLITE_PREFIX  := $(shell brew --prefix sqlite 2>/dev/null)
SQLITE_CFLAGS  := $(shell pkg-config --cflags sqlite3 2>/dev/null) $(if $(SQLITE_PREFIX),-I$(SQLITE_PREFIX)/include,-I/opt/homebrew/opt/sqlite/include)
SQLITE_LIBS    := $(shell pkg-config --libs sqlite3 2>/dev/null) $(if $(SQLITE_PREFIX),-L$(SQLITE_PREFIX)/lib,-L/opt/homebrew/opt/sqlite/lib) -lsqlite3
# ONNX Runtime: try pkg-config, then Linux /usr paths, then Homebrew
# brew only answers on macOS. Fall back to a vendored SDK before Homebrew paths.
BONFYRE_VENDOR ?= /opt/bonfyre/vendor
ONNX_VENDOR    := $(firstword $(wildcard $(BONFYRE_VENDOR)/onnxruntime/onnxruntime-linux-x64-*))
ONNX_PREFIX    := $(shell brew --prefix onnxruntime 2>/dev/null)
ONNX_CFLAGS    := $(shell pkg-config --cflags onnxruntime 2>/dev/null) $(if $(ONNX_VENDOR),-I$(ONNX_VENDOR)/include,$(if $(ONNX_PREFIX),-I$(ONNX_PREFIX)/include/onnxruntime,-I/opt/homebrew/include/onnxruntime))
ONNX_LIBS      := $(shell pkg-config --libs onnxruntime 2>/dev/null) $(if $(ONNX_VENDOR),-L$(ONNX_VENDOR)/lib -Wl$(comma)-rpath$(comma)$(ONNX_VENDOR)/lib,$(if $(ONNX_PREFIX),-L$(ONNX_PREFIX)/lib)) -lonnxruntime
LOCAL_CFLAGS = $(ONNX_CFLAGS) $(SQLITE_CFLAGS) -DSQLITE_ENABLE_LOAD_EXTENSION -I../../lib/libbonfyre/include -I../../engine/core/include
DEAD_STRIP := $(if $(filter Darwin,$(shell uname -s)),-Wl$(comma)-dead_strip,-Wl$(comma)--gc-sections)
LDFLAGS ?= $(DEAD_STRIP) $(ONNX_LIBS) $(SQLITE_LIBS) -lm ../../engine/core/libbonfyre-fabric.a -L../../lib/libbonfyre -lbonfyre
TARGET = bonfyre-embed
SRC = src/main.c

all: $(TARGET)

test: $(TARGET)
	sh tests/tokenizer_visibility_test.sh ./$(TARGET)
	sh tests/store_contention_test.sh ./$(TARGET)
	sh tests/agreement_test.sh ./$(TARGET)
	bun test tests/cache_identity.test.mjs
	bun test tests/static_embed.test.mjs

$(TARGET): $(SRC) src/static_embed.h ../../lib/libbonfyre/include/bf_bert_fold.h ../../lib/libbonfyre/libbonfyre.a ../../engine/core/libbonfyre-fabric.a
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -Wl,-dead_strip -o $(TARGET) $(SRC) $(LDFLAGS)

MANAGED_PREFIX ?= $(HOME)/.bonfyre
install: all
	install -d "$(MANAGED_PREFIX)/bin"
	install -m 755 $(TARGET) "$(MANAGED_PREFIX)/bin/.bonfyre-embed.next"
	mv -f "$(MANAGED_PREFIX)/bin/.bonfyre-embed.next" "$(MANAGED_PREFIX)/bin/bonfyre-embed"

clean:
	rm -f $(TARGET)

.PHONY: all clean test install
