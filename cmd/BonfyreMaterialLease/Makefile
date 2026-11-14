CC := cc
CFLAGS := -O2 -Wall -Wextra -std=c11 -I../../lib/libbonfyre/include
LDFLAGS := -lsqlite3

SRC := src/main.c
BUILD := build
BIN := bonfyre-material-lease

.PHONY: all clean test

all: $(BUILD)/$(BIN)

$(BUILD)/$(BIN): $(SRC) ../../lib/libbonfyre/include/bf_command_lookup.h | $(BUILD)
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

$(BUILD):
	mkdir -p $(BUILD)

test: $(BUILD)/$(BIN)
	sh tests/material_lease_test.sh $(BUILD)/$(BIN)

clean:
	rm -rf $(BUILD)
