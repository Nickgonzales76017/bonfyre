# BonfyreRecipe — Recipe Registry

CC := cc
CFLAGS := -O2 -Wall -Wextra -std=c11 -I../../lib/libbonfyre/include
LDFLAGS := -L../../lib/libbonfyre -lbonfyre -lsqlite3

SRC := src/main.c
BUILD := build
BIN := bonfyre-recipe

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

test: $(BUILD)/$(BIN)
	@echo "Testing BonfyreRecipe..."
	./$(BUILD)/$(BIN) init --db /tmp/test-recipes.db
	./$(BUILD)/$(BIN) add ../../docs/recipes/A1-audio-quick-brief.json --db /tmp/test-recipes.db
	./$(BUILD)/$(BIN) list --db /tmp/test-recipes.db
	@echo "✓ Tests passed"
