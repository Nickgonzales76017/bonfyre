CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LDFLAGS = -L../../lib/libbonfyre -lbonfyre

all: bonfyre-orchestrate

bonfyre-orchestrate: src/main.c
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $@ src/main.c $(LDFLAGS)

clean:
	rm -f bonfyre-orchestrate

.PHONY: all clean
