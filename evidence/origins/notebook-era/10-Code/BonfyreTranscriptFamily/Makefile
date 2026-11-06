CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11

all: bonfyre-transcript-family

bonfyre-transcript-family: src/main.c
	$(CC) $(CFLAGS) -o $@ src/main.c

clean:
	rm -f bonfyre-transcript-family
