CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11

BIN = bonfyre-wire
SRC = src/main.c src/wire.c

all: $(BIN)

$(BIN): $(SRC)
	$(CC) $(CFLAGS) -I../../lib/libbonfyre/include -o $@ $(SRC) ../../lib/libbonfyre/libbonfyre.a -lsqlite3

clean:
	rm -f $(BIN)

.PHONY: all clean
