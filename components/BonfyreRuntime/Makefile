CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11

all: bonfyre-runtime

bonfyre-runtime: src/main.c
	$(CC) $(CFLAGS) -o $@ src/main.c

clean:
	rm -f bonfyre-runtime
