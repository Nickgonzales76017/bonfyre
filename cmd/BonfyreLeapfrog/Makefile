CC      = cc
CFLAGS  ?= -O2 -Wall -Wextra -std=c11
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LDFLAGS = -L../../lib/libbonfyre -lbonfyre -lm
BIN     = bonfyre-leapfrog

all: $(BIN)

$(BIN): src/main.c ../../lib/libbonfyre/libbonfyre.a
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $(BIN) src/main.c $(LDFLAGS)

clean:
	rm -f $(BIN)

.PHONY: all clean
