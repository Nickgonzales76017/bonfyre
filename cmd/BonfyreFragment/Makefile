CC      := cc
CFLAGS  := -O2 -Wall -Wextra -std=c11 \
           -I../../lib/libfragment/include \
           -I/opt/homebrew/opt/openssl@3/include \
           -I/opt/homebrew/opt/sqlite/include
LDFLAGS := -L../../lib/libfragment -lfragment \
           -L/opt/homebrew/opt/openssl@3/lib -lssl -lcrypto \
           -L/opt/homebrew/opt/sqlite/lib -lsqlite3

SRC   := src/main.c
BUILD := build
BIN   := bonfyre-fragment

.PHONY: all clean install test

all: $(BUILD)/$(BIN)

$(BUILD)/$(BIN): $(SRC) ../../lib/libfragment/libfragment.a | $(BUILD)
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

../../lib/libfragment/libfragment.a:
	$(MAKE) -C ../../lib/libfragment

$(BUILD):
	mkdir -p $(BUILD)

clean:
	rm -rf $(BUILD)

install: $(BUILD)/$(BIN)
	mkdir -p ~/bin
	cp $(BUILD)/$(BIN) ~/bin/
	@echo "Installed to ~/bin/$(BIN)"

test: $(BUILD)/$(BIN)
	@echo "=== BonfyreFragment smoke tests ==="
	@rm -f /tmp/bf-frag-smoke.db /tmp/bf-frag-b.db
	@# create + get
	$(eval ID := $(shell ./$(BUILD)/$(BIN) create \
	    --store /tmp/bf-frag-smoke.db \
	    --kind claim --persp whisper-base --conf 0.9 \
	    --start-ms 1000 --end-ms 5000 \
	    --payload '{"text":"the quick brown fox"}'))
	@echo "Created: $(ID)"
	@./$(BUILD)/$(BIN) get --store /tmp/bf-frag-smoke.db --id "$(ID)" | head -5
	@echo ""
	@# query table
	./$(BUILD)/$(BIN) create --store /tmp/bf-frag-smoke.db \
	    --kind claim --persp gpt-4 --conf 0.7 \
	    --start-ms 1000 --end-ms 5000 \
	    --payload '{"text":"quick brown fox jumped"}' > /dev/null
	@./$(BUILD)/$(BIN) query --store /tmp/bf-frag-smoke.db
	@echo ""
	@# diff
	@./$(BUILD)/$(BIN) create --store /tmp/bf-frag-b.db \
	    --kind claim --persp gemini --conf 0.6 \
	    --start-ms 1000 --end-ms 5000 \
	    --payload '{"text":"fox jumped over something"}' > /dev/null
	@./$(BUILD)/$(BIN) diff --store-a /tmp/bf-frag-smoke.db --store-b /tmp/bf-frag-b.db
	@echo ""
	@# merge
	@./$(BUILD)/$(BIN) merge --dst /tmp/bf-frag-smoke.db --src /tmp/bf-frag-b.db
	@echo ""
	@# stats
	@./$(BUILD)/$(BIN) stats --store /tmp/bf-frag-smoke.db
	@echo ""
	@# ingest from BonfyreRun manifest
	@./$(BUILD)/$(BIN) ingest --store /tmp/bf-frag-smoke.db \
	    --from-run-manifest /tmp/bonfyre-v1-smoke-run/run-manifest.json 2>/dev/null || \
	    echo "(no run manifest at /tmp/bonfyre-v1-smoke-run/run-manifest.json — skipping ingest test)"
	@echo "=== All smoke tests done ==="
