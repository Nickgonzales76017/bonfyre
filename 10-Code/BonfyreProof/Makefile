CC=cc
CFLAGS=-O2 -Wall -Wextra -std=c11

all: bonfyre-proof

bonfyre-proof: src/main.c
	$(CC) $(CFLAGS) -o bonfyre-proof src/main.c

clean:
	rm -f bonfyre-proof
