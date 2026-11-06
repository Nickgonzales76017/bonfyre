CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
CFLAGS += -I../../cmd/BonfyreLex/src

all: bonfyre-distribute

bonfyre-distribute: src/main.c
	$(CC) $(CFLAGS) -o bonfyre-distribute src/main.c -lsqlite3

clean:
	rm -f bonfyre-distribute
