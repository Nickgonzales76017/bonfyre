CC=cc
CFLAGS=-O2 -Wall -Wextra -std=c11

all: bonfyre-pack

bonfyre-pack: src/main.c
	$(CC) $(CFLAGS) -o bonfyre-pack src/main.c

clean:
	rm -f bonfyre-pack
