CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
FRAGMENT_INC = ../../lib/libfragment/include
FRAGMENT_LIB = ../../lib/libfragment
OPENSSL_CFLAGS := $(shell pkg-config --cflags openssl  2>/dev/null || echo "-I/opt/homebrew/opt/openssl@3/include")
OPENSSL_LIBS   := $(shell pkg-config --libs   openssl  2>/dev/null || echo "-L/opt/homebrew/opt/openssl@3/lib -lssl -lcrypto")
SQLITE_CFLAGS  := $(shell pkg-config --cflags sqlite3  2>/dev/null || echo "")
SQLITE_LIBS    := $(shell pkg-config --libs   sqlite3  2>/dev/null || echo "-L/opt/homebrew/opt/sqlite/lib -lsqlite3")
# pkg-config, then a vendored tree, then Homebrew. Without the middle branch the
# Linux host had no way to find whisper even with it checked out under vendor/.
BONFYRE_VENDOR ?= /opt/bonfyre/vendor
WHISPER_DIR    := $(BONFYRE_VENDOR)/whisper.cpp
WHISPER_INC    := $(shell pkg-config --cflags whisper 2>/dev/null || \
                    (test -f $(WHISPER_DIR)/include/whisper.h && \
                     echo "-I$(WHISPER_DIR)/include -I$(WHISPER_DIR)/ggml/include") || \
                    echo "-I/opt/homebrew/include")
WHISPER_LIBS   := $(shell pkg-config --libs whisper 2>/dev/null || \
                    (test -d $(WHISPER_DIR)/build/bin && \
                     echo "-L$(WHISPER_DIR)/build/bin -Wl,-rpath,$(WHISPER_DIR)/build/bin -lwhisper -lggml") || \
                    echo "-L/opt/homebrew/lib -lwhisper -lggml")
GGML_CFLAGS    := $(shell pkg-config --cflags ggml 2>/dev/null || \
                    (test -d $(WHISPER_DIR)/ggml/include && echo "-I$(WHISPER_DIR)/ggml/include") || \
                    echo "-I/opt/homebrew/include")
GGML_LIBS      := $(shell pkg-config --libs ggml 2>/dev/null || echo "-L/opt/homebrew/lib -lggml")
LOCAL_CFLAGS = -I../../lib/libbonfyre/include \
               -I$(FRAGMENT_INC) \
               $(OPENSSL_CFLAGS) $(SQLITE_CFLAGS) $(WHISPER_INC) $(GGML_CFLAGS)
LDFLAGS  = -L../../lib/libbonfyre -lbonfyre $(WHISPER_LIBS) $(GGML_LIBS) -lz -lm \
           -L$(FRAGMENT_LIB) -lfragment \
           $(OPENSSL_LIBS) \
           $(SQLITE_LIBS)
TARGET = bonfyre-transcribe
SRC = src/main.c

all: $(FRAGMENT_LIB)/libfragment.a $(TARGET)

MANAGED_PREFIX ?= $(HOME)/.bonfyre
install: all
	install -d "$(MANAGED_PREFIX)/bin"
	install -m 755 $(TARGET) "$(MANAGED_PREFIX)/bin/.bonfyre-transcribe.next"
	mv -f "$(MANAGED_PREFIX)/bin/.bonfyre-transcribe.next" "$(MANAGED_PREFIX)/bin/bonfyre-transcribe"

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

$(FRAGMENT_LIB)/libfragment.a:
	$(MAKE) -C $(FRAGMENT_LIB)

test: $(TARGET)
	$(MAKE) -C ../../tests/vectors bonfyre-vectors
	cd ../.. && tests/vectors/bonfyre-vectors --root . cmd/BonfyreTranscribe/tests/vectors/command_behavior.json cmd/BonfyreTranscribe/tests/vectors/caption_contract.json
	@$(MAKE) -s caption-mutation-witness

.PHONY: caption-mutation-witness
caption-mutation-witness:
	@log=$(mktemp); trap 'rm -f "$log"' EXIT; \
	if BONFYRE_TRANSCRIBE_BIN=/usr/bin/false ../../tests/vectors/bonfyre-vectors --root ../.. tests/vectors/caption_contract.json >"$log" 2>&1; then \
	  echo 'REFUSE: mutated transcription behavior passed caption contract' >&2; exit 1; \
	fi; \
	if ! grep -q '^  FAIL' "$log"; then cat "$log" >&2; exit 1; fi; \
	echo 'PASS caption mutation refusal'

clean:
	rm -f $(TARGET)

.PHONY: all clean install test
