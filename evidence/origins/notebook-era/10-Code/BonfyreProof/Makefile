CC=cc
CFLAGS=-O2 -Wall -Wextra -std=c11
CFLAGS += -I../../cmd/BonfyreLex/src

all: bonfyre-proof

bonfyre-proof: src/main.c
	$(CC) $(CFLAGS) -o bonfyre-proof src/main.c

clean:
	rm -f bonfyre-proof
