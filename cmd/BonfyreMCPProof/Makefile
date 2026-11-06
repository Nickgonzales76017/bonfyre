CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
ROOT := ../..
LIBBONFYRE := $(ROOT)/lib/libbonfyre
PROGRAM := bonfyre-mcp-proof
SRC := src/main.c src/util.c src/json.c src/context.c src/cms.c src/graph.c src/flow.c src/discipl.c src/report.c $(LIBBONFYRE)/src/bf_sha256.c

.PHONY: all test clean

all: $(PROGRAM)

$(PROGRAM): $(SRC)
	$(CC) $(CFLAGS) -Isrc -I$(LIBBONFYRE)/include -o $@ $(SRC) -lsqlite3

test: $(PROGRAM)
	./tests/live_generation_proof.sh

clean:
	rm -f $(PROGRAM)
