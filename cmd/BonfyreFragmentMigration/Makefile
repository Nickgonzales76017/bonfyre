CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
ROOT := ../..
LIB := $(ROOT)/lib/libbonfyre
PROGRAM := bonfyre-fragment-migration
SRC := src/main.c src/compile.c src/admit.c src/util.c $(LIB)/src/bf_sha256.c

.PHONY: all clean test

all: $(PROGRAM)

$(PROGRAM): $(SRC)
	$(CC) $(CFLAGS) -I$(LIB)/include -o $@ $(SRC) -lsqlite3

test: $(PROGRAM)
	sh tests/fragment_migration_test.sh ./$(PROGRAM)

clean:
	rm -f $(PROGRAM)
