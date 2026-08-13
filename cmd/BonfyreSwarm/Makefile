CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11 -I../../lib/libbonfyre/include

all: bonfyre-swarm

bonfyre-swarm: src/main.c
	$(CC) $(CFLAGS) -o bonfyre-swarm src/main.c ../../lib/libbonfyre/libbonfyre.a -lsqlite3

clean:
	rm -f bonfyre-swarm
