CC=cc
CFLAGS=-O2 -Wall -Wextra -std=c11

all: bonfyre-queue

bonfyre-queue: src/main.c
	$(CC) $(CFLAGS) -o bonfyre-queue src/main.c

clean:
	rm -f bonfyre-queue
