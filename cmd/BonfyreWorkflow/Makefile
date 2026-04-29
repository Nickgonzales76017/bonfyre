CC := cc
CFLAGS := -O2 -Wall -Wextra -std=c11 -I../../lib/libbonfyre/include
LDFLAGS := -L../../lib/libbonfyre -lbonfyre -lsqlite3

SRC := src/main.c
BUILD := build
BIN := bonfyre-workflow

.PHONY: all clean install

all: $(BUILD)/$(BIN)

$(BUILD)/$(BIN): $(SRC) | $(BUILD)
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
