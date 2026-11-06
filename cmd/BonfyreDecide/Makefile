# bonfyre-decide: the DecisionPlane products (books, monitor, manifest) in C.
# Replaces three `bun build --compile` executables of 63 MB each. The policy
# is the CUE export of components/BonfyreDecision/decision-plane.cue, compiled
# in as data (no cue at run time).
CC      ?= cc
LIB      = ../../lib/libbonfyre
CUE_SRC  = ../../components/BonfyreDecision/decision-plane.cue
CFLAGS  ?= -Os -Wall -Wextra -Werror -std=gnu11 -D_GNU_SOURCE -D_DEFAULT_SOURCE -D_DARWIN_C_SOURCE
CFLAGS  += -ffp-contract=off -I$(LIB)/include -Ibuild -Isrc -flto -ffunction-sections -fdata-sections
LDFLAGS += -flto
LDLIBS   = -lsqlite3 -lz -lm
ifeq ($(shell uname),Darwin)
LDFLAGS += -Wl,-dead_strip
else
LDFLAGS += -Wl,--gc-sections
endif
SRCS     = src/main.c src/dp_core.c src/dp_util.c src/dp_gguf.c src/books.c src/books_xlsx.c src/monitor.c src/manifest.c
TARGET   = bonfyre-decide
LINKS    = bonfyre-books bonfyre-monitor bonfyre-manifest

.PHONY: all clean test install

all: $(TARGET) $(LINKS)

build/policy.h: $(CUE_SRC)
	@mkdir -p build
	cue export $(CUE_SRC) -e decisionPlane > build/policy.json
	{ echo '/* generated from decision-plane.cue by make; do not edit */'; \
	  echo 'static const unsigned char dp_policy_json[] = {'; \
	  od -An -v -tx1 build/policy.json | sed 's/\([0-9a-f][0-9a-f]\)/0x\1,/g'; \
	  echo '0x00};'; } > $@

$(LIB)/libbonfyre.a:
	$(MAKE) -C $(LIB)

$(TARGET): build/policy.h $(SRCS) src/dp.h src/books.h src/usage.h $(LIB)/libbonfyre.a
	$(CC) $(CFLAGS) $(SRCS) $(LIB)/libbonfyre.a $(LDFLAGS) $(LDLIBS) -o $@

$(LINKS): $(TARGET)
	ln -sf $(TARGET) $@

test: all
	sh tests/parity.sh

# Installed names are hard links (one inode): MCP native discovery refuses
# symlinks, and a copy would spend the bytes three more times.
install: all
	@dest=$${DEST:-$$HOME/.bonfyre/bin}; mkdir -p "$$dest"; \
	install -m 755 $(TARGET) "$$dest/.$(TARGET).new" && mv -f "$$dest/.$(TARGET).new" "$$dest/$(TARGET)"; \
	for l in $(LINKS); do ln -f "$$dest/$(TARGET)" "$$dest/.$$l.new" && mv -f "$$dest/.$$l.new" "$$dest/$$l"; done; \
	echo "installed $$dest/$(TARGET) ($$(wc -c < $(TARGET) | tr -d ' ') bytes) as $(LINKS)"

clean:
	rm -rf build $(TARGET) $(LINKS)
