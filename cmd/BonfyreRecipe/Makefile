# BonfyreRecipe — Recipe Registry

CC := cc
CFLAGS := -O2 -Wall -Wextra -std=c11 -ffunction-sections -fdata-sections -I../../lib/libbonfyre/include -I../BonfyreCompress/src -I../BonfyreExternalEstate/src
LDFLAGS := -L../../lib/libbonfyre -lbonfyre -lsqlite3
ZSTD_PREFIX ?= $(shell for p in /opt/homebrew /usr/local /opt/local /usr; do [ -f $$p/include/zstd.h ] && echo $$p && break; done)
ifneq ($(ZSTD_PREFIX),)
CFLAGS += -DBONFYRE_HAVE_ZSTD -I$(ZSTD_PREFIX)/include
LDFLAGS += $(if $(wildcard $(ZSTD_PREFIX)/lib/libzstd.a),$(ZSTD_PREFIX)/lib/libzstd.a,-L$(ZSTD_PREFIX)/lib -lzstd)
endif

SRC := ../BonfyreExternalEstate/src/address_resolve.c src/main.c src/recipe_material.c src/recipe_week.c src/recipe_nutrition.c ../BonfyreCompress/src/record_factor.c
BUILD := build
BIN := bonfyre-recipe
HEADERS := ../BonfyreExternalEstate/src/address_resolve.h src/recipe_material.h src/recipe_week.h src/recipe_nutrition.h ../BonfyreCompress/src/record_factor.h

.PHONY: all clean install

all: $(BUILD)/$(BIN)

$(BUILD)/$(BIN): $(SRC) $(HEADERS) | $(BUILD)
	$(CC) $(CFLAGS) -Wl,-dead_strip -o $@ $(SRC) $(LDFLAGS)

$(BUILD):
	mkdir -p $(BUILD)

clean:
	rm -rf $(BUILD)

install: $(BUILD)/$(BIN)
	mkdir -p ~/bin
	cp $(BUILD)/$(BIN) ~/bin/
	@echo "Installed to ~/bin/$(BIN)"
	@echo "Make sure ~/bin is in your PATH"

.PHONY: material-witness
$(BUILD)/material-driver: tests/material_driver.c src/recipe_material.c ../BonfyreCompress/src/record_factor.c $(HEADERS) | $(BUILD)
	$(CC) $(CFLAGS) -Isrc -o $@ tests/material_driver.c src/recipe_material.c ../BonfyreCompress/src/record_factor.c $(LDFLAGS)
material-witness: $(BUILD)/$(BIN) $(BUILD)/material-driver
	bun tests/material.test.mjs ./$(BUILD)/$(BIN) ./$(BUILD)/material-driver

.PHONY: nutrition-witness
nutrition-witness: $(BUILD)/$(BIN)
	bun test tests/nutrition.test.mjs

test: $(BUILD)/$(BIN)
	@echo "Testing BonfyreRecipe..."
	./$(BUILD)/$(BIN) init --db /tmp/test-recipes.db
	./$(BUILD)/$(BIN) add ../../docs/recipes/A1-audio-quick-brief.json --db /tmp/test-recipes.db
	./$(BUILD)/$(BIN) list --db /tmp/test-recipes.db
	@echo "✓ Tests passed"
test: nutrition-witness
