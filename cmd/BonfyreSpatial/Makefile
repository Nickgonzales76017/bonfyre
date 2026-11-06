CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
CFLAGS += -I../BonfyreLex/src

TARGET := bonfyre-spatial

.PHONY: all clean test native-witness
all: $(TARGET) native-witness

ROOT := ../..
LIB_SOURCES := $(ROOT)/lib/libbonfyre/src/bf_sha256.c
LIB_CFLAGS := -I$(ROOT)/lib/libbonfyre/include

$(TARGET): src/main.c $(LIB_SOURCES) $(ROOT)/lib/libbonfyre/include/bf_sqlite_path.h $(ROOT)/lib/libbonfyre/include/bf_command_lookup.h
	$(CC) $(CFLAGS) $(LIB_CFLAGS) -o $@ src/main.c $(LIB_SOURCES) -lsqlite3 -lm

native-witness: $(TARGET)
	./tests/spatial_test.sh ./$(TARGET)

test: native-witness

clean:
	rm -f $(TARGET)
