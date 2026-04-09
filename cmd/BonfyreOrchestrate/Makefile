CC ?= cc
CFLAGS ?= -Os -Wall -Wextra -std=c11 -ffunction-sections -fdata-sections
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LDFLAGS = -L../../lib/libbonfyre -lbonfyre -lsqlite3

all: bonfyre-orchestrate

bonfyre-orchestrate: src/main.c
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $@ src/main.c $(LDFLAGS)
	strip -x $@ 2>/dev/null || true

clean:
	rm -f bonfyre-orchestrate

.PHONY: all clean
