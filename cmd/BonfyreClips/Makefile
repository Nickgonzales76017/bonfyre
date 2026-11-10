CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LDFLAGS  = -L../../lib/libbonfyre -lbonfyre -lm
TARGET = bonfyre-clips
SRC = src/main.c

all: $(TARGET) test

MANAGED_PREFIX ?= $(HOME)/.bonfyre
install-export: $(TARGET)
	install -d "$(MANAGED_PREFIX)/bin" "$(MANAGED_PREFIX)/lib/clips"
	install -m 644 tools/export_clip.mjs "$(MANAGED_PREFIX)/lib/clips/export_clip.mjs"
	install -m 644 tools/cut_clip.mjs "$(MANAGED_PREFIX)/lib/clips/cut_clip.mjs"
	install -m 644 tools/harvest_clips.mjs "$(MANAGED_PREFIX)/lib/clips/harvest_clips.mjs"
	install -m 755 $(TARGET) "$(MANAGED_PREFIX)/bin/bonfyre-clips"

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

clean:
	rm -f $(TARGET)

.PHONY: all clean install-export test

test: $(TARGET)
	bun test tests/hash_file.test.mjs tests/export_clip.test.mjs tests/cut_clip.test.mjs tests/harvest_clips.test.mjs
