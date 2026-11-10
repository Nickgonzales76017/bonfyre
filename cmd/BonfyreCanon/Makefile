CC       ?= cc
CFLAGS   ?= -O2 -Wall -Wextra -std=c11
CFLAGS += -I../BonfyreLex/src
TS_CFLAGS := $(shell pkg-config --cflags tree-sitter 2>/dev/null || echo "-I/opt/homebrew/include")
TS_LIBS   := $(shell pkg-config --libs   tree-sitter 2>/dev/null || echo "-L/opt/homebrew/lib -ltree-sitter")
LOCAL_CFLAGS = $(TS_CFLAGS) -Isrc -I../../lib/libbonfyre/include
LDFLAGS  = -L../../lib/libbonfyre -lbonfyre $(TS_LIBS)
TARGET    = bonfyre-canon
SRC       = src/main.c src/c_literals.c src/ts_json_parser.c
SYNC_BIN  = $(HOME)/.bonfyre/bin/$(TARGET)

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

install-bin: $(TARGET)
	@mkdir -p $(dir $(SYNC_BIN))
	@cp $(TARGET) $(SYNC_BIN)
	@codesign --force --sign - $(SYNC_BIN) >/dev/null 2>&1 || true

clean:
	rm -f $(TARGET)
