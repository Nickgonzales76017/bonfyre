CC := cc
CFLAGS := -O2 -Wall -Wextra -std=c11 -I../../lib/libbonfyre/include
CFLAGS += -I../BonfyreLex/src
LDFLAGS := -L../../lib/libbonfyre -lbonfyre -lsqlite3

SRC := src/main.c
BUILD := build
BIN := bonfyre-family

.PHONY: all clean install

all: $(BUILD)/$(BIN)

$(BUILD)/$(BIN): $(SRC) ../../lib/libbonfyre/include/bf_command_lookup.h | $(BUILD)
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

$(BUILD):
	mkdir -p $(BUILD)

clean:
	rm -rf $(BUILD)

install: $(BUILD)/$(BIN)
	mkdir -p ~/bin
	cp $(BUILD)/$(BIN) ~/bin/
	@echo "Installed to ~/bin/$(BIN)"
	@echo "Make sure ~/bin is in your PATH"
