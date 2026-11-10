CC := cc
CFLAGS := -O2 -Wall -Wextra -std=c11 -I../../lib/libbonfyre/include
CFLAGS += -I../BonfyreLex/src
LDFLAGS := -L../../lib/libbonfyre -lbonfyre -lsqlite3

SRC := src/main.c
BUILD := build
BIN := bonfyre-workflow

.PHONY: all clean install test

MANAGED_PREFIX ?= $(HOME)/.bonfyre

all: $(BUILD)/$(BIN)

$(BUILD)/$(BIN): $(SRC) | $(BUILD)
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

$(BUILD):
	mkdir -p $(BUILD)

clean:
	rm -rf $(BUILD)

test: $(BUILD)/$(BIN)
	cd ../.. && tests/vectors/bonfyre-vectors --root . cmd/BonfyreWorkflow/tests/vectors/command_behavior.json

install: $(BUILD)/$(BIN)
	install -d "$(MANAGED_PREFIX)/bin"
	install -m 755 $(BUILD)/$(BIN) "$(MANAGED_PREFIX)/bin/.bonfyre-workflow.next"
	mv -f "$(MANAGED_PREFIX)/bin/.bonfyre-workflow.next" "$(MANAGED_PREFIX)/bin/bonfyre-workflow"
