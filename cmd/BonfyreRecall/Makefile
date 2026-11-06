CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
ROOT := ../..
LIBBONFYRE := $(ROOT)/lib/libbonfyre
TARGET = bonfyre-recall
SRC = src/main.c src/util.c src/query.c src/index.c $(LIBBONFYRE)/src/bf_sha256.c

.PHONY: all clean test

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -I$(LIBBONFYRE)/include -o $@ $(SRC) -lsqlite3

test: $(TARGET)
	./tests/semantic_recall_test.sh ./$(TARGET)

clean:
	rm -f $(TARGET)
