CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
ROOT := ../..
LIB := $(ROOT)/lib/libbonfyre
TARGET = bonfyre-obsidian
SRC = src/main.c src/vault.c src/drive.c
LIBSRC = $(LIB)/bf_jsonw.c $(LIB)/bf_json.c $(LIB)/src/bf_sha256.c

all: $(TARGET)

$(TARGET): $(SRC) src/obsidian.h $(LIBSRC)
	$(CC) $(CFLAGS) -I$(LIB)/include -o $@ $(SRC) $(LIBSRC) -lsqlite3

test: $(TARGET)
	sh tests/obsidian_test.sh ./$(TARGET)

# Read-only run against a real vault on this machine; never writes to it.
# REAL_VAULT=/path/to/vault make real
real: $(TARGET)
	sh tests/real_readonly_test.sh ./$(TARGET) "$(REAL_VAULT)"

clean:
	rm -f $(TARGET)

.PHONY: all test real clean
