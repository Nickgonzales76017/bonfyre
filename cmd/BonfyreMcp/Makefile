# bonfyre-mcp: the native MCP organ (W2). First slice: owner command clients
# for spines, canary, recursive and v6, replacing their retired .mjs CLIs.
CC      ?= cc
LIB      = ../../lib/libbonfyre
CFLAGS  ?= -Os -Wall -Wextra -Werror -std=gnu11 -D_GNU_SOURCE -D_DEFAULT_SOURCE -D_DARWIN_C_SOURCE
CFLAGS  += -ffp-contract=off -I$(LIB)/include -flto -ffunction-sections -fdata-sections
LDFLAGS += -flto
ifeq ($(shell uname),Darwin)
LDFLAGS += -Wl,-dead_strip
else
LDFLAGS += -Wl,--gc-sections
endif
TARGET   = bonfyre-mcp
SRC      = src/main.c src/jsx.c src/wire.c src/schema.c src/clis.c src/fixture.c

.PHONY: all clean test install

all: $(TARGET)

$(LIB)/libbonfyre.a:
	$(MAKE) -C $(LIB)

$(TARGET): $(SRC) src/mcp.h $(LIB)/libbonfyre.a
	$(CC) $(CFLAGS) $(SRC) $(LIB)/libbonfyre.a $(LDFLAGS) -lm -o $@

test: $(TARGET)
	bash tests/parity.sh

install: $(TARGET)
	@dest=$${DEST:-$$HOME/.bonfyre/bin}; mkdir -p "$$dest"; \
	install -m 755 $(TARGET) "$$dest/.$(TARGET).new" && mv -f "$$dest/.$(TARGET).new" "$$dest/$(TARGET)"; \
	echo "installed $$dest/$(TARGET)"

clean:
	rm -f $(TARGET)
