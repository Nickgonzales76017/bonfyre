CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
CFLAGS += -I../../cmd/BonfyreLex/src
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LDFLAGS  = -L../../lib/libbonfyre -lbonfyre

all: bonfyre-offer

bonfyre-offer: src/main.c
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o bonfyre-offer src/main.c $(LDFLAGS)

test: bonfyre-offer
	sh tests/commercial_safety_test.sh

clean:
	rm -f bonfyre-offer

.PHONY: all test clean
