CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11

all: bonfyre-swarm

bonfyre-swarm: src/main.c
	$(CC) $(CFLAGS) -o bonfyre-swarm src/main.c -lsqlite3

clean:
	rm -f bonfyre-swarm
