CC ?= cc
# An include path is not a tuning preference. `make all` passes CFLAGS through
# the environment, which makes `?=` skip this whole line -- so every -I on it
# vanished and the organ could not be rebuilt from scratch by the top-level
# build, only by its own. Optimisation stays overridable; what the source needs
# to compile at all is appended.
CFLAGS ?= -O2 -Wall -Wextra -std=c11
CFLAGS += -I../../lib/libbonfyre/include

all: bonfyre-swarm

bonfyre-swarm: src/main.c src/scoped_transfer.h
	$(CC) $(CFLAGS) -o bonfyre-swarm src/main.c ../../lib/libbonfyre/libbonfyre.a -lsqlite3

# Uses the actual native SHA, crypto and MessagePack owners. Each test binds
# only loopback and creates all grants, bytes and receipts in a temporary dir.
test-scoped-transfer:
	python3 tests/scoped_transfer_test.py

clean:
	rm -f bonfyre-swarm

.PHONY: all clean test-scoped-transfer
