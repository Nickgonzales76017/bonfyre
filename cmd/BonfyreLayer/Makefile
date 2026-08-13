CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LDFLAGS = -L../../lib/libbonfyre -lbonfyre -lsqlite3
TARGETS = bonfyre-layer bonfyre-layer-c
SRC = src/main.c

all: $(TARGETS)

$(TARGETS): $(SRC)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $@ $(SRC) $(LDFLAGS)

clean:
	rm -f $(TARGETS)

.PHONY: all clean
