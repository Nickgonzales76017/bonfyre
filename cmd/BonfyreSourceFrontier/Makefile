CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
ROOT := ../..
TARGET = bonfyre-source-frontier
STATIC_MANIFEST = tools/source-ledger-static-manifest
CIVIC_LEDGER = tools/civic-ledger
MANAGED_PREFIX ?= $(HOME)/.bonfyre

all: $(TARGET) $(STATIC_MANIFEST) $(CIVIC_LEDGER)

# Retained material and multiple exact feed-item provenance are release gates.
.PHONY: source-material-witness
all test: source-material-witness
source-material-witness:
	bun test ./tests/source_material.test.mjs

.PHONY: public-selection-index-witness known-source-lane-witness
all test: public-selection-index-witness known-source-lane-witness
known-source-lane-witness:
	bun test ../../scripts/release/tests/source_library_known_upgrade.test.mjs
	bun test --timeout 90000 ../../scripts/release/tests/source_library_known_browser.test.mjs
public-selection-index-witness: tests/public_selection_index_test.c src/main.c
	@set -eu; witness=$$(mktemp "$${TMPDIR:-/tmp}/frontier-index-test.XXXXXX"); \
	trap 'rm -f "$$witness"' 0 1 2 15; \
	$(CC) $(CFLAGS) -I$(ROOT)/lib/libbonfyre/include tests/public_selection_index_test.c $(ROOT)/lib/libbonfyre/bf_json.c $(ROOT)/lib/libbonfyre/src/bf_sha256.c -lsqlite3 -o "$$witness"; "$$witness"

install-classified-market: $(TARGET)
	install -d "$(MANAGED_PREFIX)/bin" "$(MANAGED_PREFIX)/lib/source-frontier"
	install -m 644 tools/operator_supply.mjs tools/foreign_observation.mjs "$(MANAGED_PREFIX)/lib/source-frontier/"
	install -m 644 tools/classified_market.mjs "$(MANAGED_PREFIX)/lib/source-frontier/.classified_market.mjs.next"
	mv -f "$(MANAGED_PREFIX)/lib/source-frontier/.classified_market.mjs.next" "$(MANAGED_PREFIX)/lib/source-frontier/classified_market.mjs"
	install -m 755 $(TARGET) "$(MANAGED_PREFIX)/bin/.$(TARGET).next"
	mv -f "$(MANAGED_PREFIX)/bin/.$(TARGET).next" "$(MANAGED_PREFIX)/bin/$(TARGET)"

# Install only this reader and its native entry point; other projections stay put.
install-discourse-review: $(TARGET)
	install -d "$(MANAGED_PREFIX)/bin" "$(MANAGED_PREFIX)/lib/source-frontier"
	install -m 644 tools/discourse_thread.mjs "$(MANAGED_PREFIX)/lib/source-frontier/.discourse_thread.mjs.next"
	mv -f "$(MANAGED_PREFIX)/lib/source-frontier/.discourse_thread.mjs.next" "$(MANAGED_PREFIX)/lib/source-frontier/discourse_thread.mjs"
	install -m 755 $(TARGET) "$(MANAGED_PREFIX)/bin/.$(TARGET).next"
	mv -f "$(MANAGED_PREFIX)/bin/.$(TARGET).next" "$(MANAGED_PREFIX)/bin/$(TARGET)"

$(TARGET): src/main.c $(ROOT)/lib/libbonfyre/bf_json.c $(ROOT)/lib/libbonfyre/src/bf_sha256.c $(ROOT)/lib/libbonfyre/include/bf_sqlite_path.h $(ROOT)/lib/libbonfyre/include/bf_command_lookup.h
	$(CC) $(CFLAGS) -I$(ROOT)/lib/libbonfyre/include -o $@ src/main.c $(ROOT)/lib/libbonfyre/bf_json.c $(ROOT)/lib/libbonfyre/src/bf_sha256.c -lsqlite3

$(STATIC_MANIFEST): tools/source_ledger_static_manifest.c $(ROOT)/lib/libbonfyre/src/bf_sha256.c $(ROOT)/lib/libbonfyre/include/bf_static_query.h
	$(CC) $(CFLAGS) -I$(ROOT)/lib/libbonfyre/include -o $@ tools/source_ledger_static_manifest.c $(ROOT)/lib/libbonfyre/src/bf_sha256.c -lsqlite3

$(CIVIC_LEDGER): tools/civic_ledger.c $(ROOT)/lib/libbonfyre/bf_json.c $(ROOT)/lib/libbonfyre/src/bf_sha256.c
	$(CC) $(CFLAGS) -D_DARWIN_C_SOURCE -I$(ROOT)/lib/libbonfyre/include -o $@ tools/civic_ledger.c $(ROOT)/lib/libbonfyre/bf_json.c $(ROOT)/lib/libbonfyre/src/bf_sha256.c

.PHONY: civic-ledger-test
civic-ledger-test: $(CIVIC_LEDGER)
	sh ./tests/civic_ledger_test.sh ./$(CIVIC_LEDGER)

test: $(TARGET) $(STATIC_MANIFEST) $(CIVIC_LEDGER)
	sh ./tests/civic_ledger_test.sh ./$(CIVIC_LEDGER)
	./tests/source_frontier_test.sh ./$(TARGET)
	bun ./tests/discover_with_exa_test.mjs
	bun ./tests/sweep_root_grammars_test.mjs
	bun ./tests/probe_catalogues_test.mjs
	bun ./tests/qualify_channels_test.mjs
	bun test ./tests/expand_retained_channels_test.mjs
	bun test ./tests/expand_publisher_lattice_test.mjs
	bun test ./tests/expand_publishers_wikidata.test.mjs
	bun test ./tests/dork_common_crawl.test.mjs
	bun test ./tests/publisher_grammar_policy.test.mjs
	bun test ./tests/compile_inventories_test.mjs
	bun test ./tests/probe_event_channels_test.mjs
	bun test ./tests/expand_catalogues_test.mjs
	bun test ./tests/close_gaps_freely_test.mjs
	bun ../../scripts/tests/firecrawl_second_look_test.mjs
	bun ../../scripts/tests/resolve_program_sources_test.mjs
	bun ../../scripts/tests/qualify_claims_test.mjs
	bun test ./tests/channel_hierarchy.test.mjs
	bun test ./tests/classified_market.test.mjs ./tests/operator_supply.test.mjs
	bun test ./tests/discourse_thread.test.mjs
	sh ./tests/source_ledger_static_manifest_test.sh
	sh ../../scripts/tests/source_ledger_static_export_test.sh

clean:
	rm -f $(TARGET) $(STATIC_MANIFEST)

.PHONY: all test clean install-discourse-review install-classified-market

.PHONY: traktok-witness
traktok-witness: $(TARGET)
	bun test --timeout 120000 tests/traktok_collect.test.mjs
test: traktok-witness
