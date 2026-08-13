CC=cc
CFLAGS=-O2 -Wall -Wextra -std=c11

all: bonfyre-distribute

bonfyre-distribute: src/main.c
	$(CC) $(CFLAGS) -o bonfyre-distribute src/main.c

clean:
	rm -f bonfyre-distribute
