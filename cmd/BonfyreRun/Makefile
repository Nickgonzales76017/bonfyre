# BonfyreRun — Recipe Executor

CC := cc
CFLAGS := -O2 -Wall -Wextra -std=c11 -I/opt/homebrew/opt/sqlite/include -I../../lib/libbonfyre/include
LDFLAGS := -L../../lib/libbonfyre -lbonfyre -L/opt/homebrew/opt/sqlite/lib -lsqlite3

SRC := src/main.c
BUILD := build
BIN := bonfyre-run

.PHONY: all clean install test

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

test: $(BUILD)/$(BIN)
	@echo "Testing BonfyreRun..."
	./$(BUILD)/$(BIN) A1 --input test.wav --out /tmp/bonfyre-test --db /tmp/test-recipes.db --dry-run
	@echo "✓ Dry-run test passed"
