CC=cc
CFLAGS=-O2 -Wall -Wextra -std=c11
CFLAGS += -I../../cmd/BonfyreLex/src

all: bonfyre-offer

bonfyre-offer: src/main.c
	$(CC) $(CFLAGS) -o bonfyre-offer src/main.c

clean:
	rm -f bonfyre-offer
