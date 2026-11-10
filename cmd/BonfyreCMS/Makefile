.DEFAULT_GOAL := all
CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11 -ffunction-sections -fdata-sections
CFLAGS += -I../../cmd/BonfyreLex/src
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LDFLAGS  = ../../lib/libbonfyre/libbonfyre.a
TARGET = bonfyre-cms
COMPACT_PROBE = tests/compact-probe

# liblambda-tensors (standalone compression library)
LT_DIR = ../../lib/liblambda-tensors
LT_INC = -I$(LT_DIR)/include
LT_LIB = $(LT_DIR)/liblambda-tensors.a

SRC = src/account_world.c src/main.c src/bench_metrics.c src/ephemeral.c src/canonical.c src/lt_egraph.c src/tensor_ops.c \
      src/incremental.c src/synthesis.c src/ann_index.c src/delta_sync.c \
      src/type_check.c src/meta_gen.c src/columnar.c src/hybrid_reduce.c \
      src/provenance.c src/transfer.c src/compact_bindings.c \
      ../../lib/libbonfyre/src/bonfyre_frappe_condition.c

HDR = src/bench_metrics.h src/ephemeral.h src/canonical.h src/lt_egraph.h src/tensor_ops.h \
      src/incremental.h src/synthesis.h src/ann_index.h src/delta_sync.h \
      src/type_check.h src/meta_gen.h src/columnar.h src/hybrid_reduce.h \
      src/provenance.h src/transfer.h src/compact_bindings.h \
      ../../lib/libbonfyre/include/bonfyre_frappe_condition.h

HDR += src/pack_lifecycle.h src/pack_image.h ../BonfyreWebAddress/src/pack_resolution.h
HDR += src/workspace_store.h src/workspace_records.h src/workspace_blobs.h src/workspace_continuation.h
HDR += src/household_access.h src/household_members.h src/page_snapshot.h src/entry_snapshot.h
HDR += src/schema_read_indexes.h
HDR += src/source_publication.h
HDR += src/community_read.h src/community_journey.h

.PHONY: read-indexes-test
all: read-indexes-test
read-indexes-test: $(TARGET) tests/read_indexes_test.c
	@set -eu; witness=$$(mktemp "$${TMPDIR:-/tmp}/cms-read-indexes.XXXXXX"); \
	trap 'rm -f "$$witness"' 0 1 2 15; \
	$(CC) $(CFLAGS) tests/read_indexes_test.c -lsqlite3 -o "$$witness"; "$$witness" "$$(pwd)/$(TARGET)"

.PHONY: pack-lifecycle-witness
pack-lifecycle-witness: $(TARGET)
	bun tests/pack_lifecycle_test.mjs
pack-image-witness: $(TARGET)
	sh tests/pack_image_test.sh ./$(TARGET)
.PHONY: pack-image-witness
all: pack-image-witness
all: pack-lifecycle-witness

SYNC_BIN = $(HOME)/.bonfyre/bin/$(TARGET)

all: $(LT_LIB) $(TARGET) subcommand-dispatch-test site-fragment-test import-jsonl-test fact-delta-test conditional-required-test large-update-test hook-held-message-test presentation-template-test world-fragments-test project-listings-owner-test publish-market-owner-test project-event-sources-test
.PHONY: publication-promotion-witness
all: publication-promotion-witness
publication-promotion-witness: $(TARGET)
	sh ./tests/publication_promotion_test.sh ./$(TARGET)

$(LT_LIB):
	$(MAKE) -C $(LT_DIR) liblambda-tensors.a

$(TARGET): $(SRC) $(HDR) ../../cmd/BonfyreLex/src/bf_lex.h ../../lib/libbonfyre/include/bf_command_lookup.h $(LT_LIB)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) $(LT_INC) -Wl,-dead_strip -o $(TARGET) $(SRC) $(LT_LIB) -lsqlite3 -lpthread -lm -lz $(LDFLAGS)

install-bin: $(TARGET)
	@mkdir -p $(dir $(SYNC_BIN))
	@cp $(TARGET) $(SYNC_BIN)
	@codesign --force --sign - $(SYNC_BIN) >/dev/null 2>&1 || true

# Witnesses already run under all; this is the uniform entry point.
.PHONY: test
test: all

$(COMPACT_PROBE): tests/compact_probe.c src/compact_bindings.c src/compact_bindings.h src/bench_metrics.c src/bench_metrics.h $(LT_LIB)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -Isrc $(LT_INC) -o $(COMPACT_PROBE) tests/compact_probe.c src/compact_bindings.c src/bench_metrics.c $(LT_LIB) -lsqlite3 -lm

compact-probe: $(COMPACT_PROBE)

site-fragment-test: $(TARGET)
	./tests/site_fragment_pipeline_test.sh ./$(TARGET)

subcommand-dispatch-test: $(TARGET)
	bun test tools/cms_subcommand_dispatch.test.mjs

import-jsonl-test: $(TARGET)
	./tests/entry_import_jsonl_test.sh ./$(TARGET)

fact-delta-test: $(TARGET)
	./tests/fact_delta_test.sh ./$(TARGET)

conditional-required-test: $(TARGET)
	./tests/conditional_required_test.sh ./$(TARGET)

large-update-test: $(TARGET)
	./tests/large_update_test.sh ./$(TARGET)

hook-held-message-test: $(TARGET)
	./tests/hook_held_message_test.sh ./$(TARGET)

presentation-template-test:
	bun test tools/ghost_marketplace_templates.test.mjs tools/ghost_public_anatomy.test.mjs

world-fragments-test:
	bun test tools/materialize_world_fragments.test.mjs

.PHONY: project-listings-owner-test
project-listings-owner-test: $(TARGET) ../BonfyreCommerce/bonfyre-commerce
	bun test ../../scripts/tests/project_listings_owner.test.mjs

../BonfyreCommerce/bonfyre-commerce:
	$(MAKE) -C ../BonfyreCommerce bonfyre-commerce

publish-market-owner-test: project-listings-owner-test
	bun test ../../scripts/tests/publish_market_owner.test.mjs

project-event-sources-test: $(TARGET)
	sh ../../scripts/tests/project_event_sources_test.sh
	sh ../../scripts/tests/project_event_sources_owner_test.sh

clean:
	rm -f $(TARGET) $(COMPACT_PROBE)

PREFIX ?= /usr/local
install: $(TARGET)
	install -d $(DESTDIR)$(PREFIX)/bin
	install -m 755 $(TARGET) $(DESTDIR)$(PREFIX)/bin/$(TARGET)

uninstall:
	rm -f $(DESTDIR)$(PREFIX)/bin/$(TARGET)

all: schema-preservation-test conditional-update-test

conditional-update-test: $(TARGET)
	bun test tests/conditional_update.test.mjs

schema-preservation-test: $(TARGET)
	bun test tests/schema_preservation.test.mjs tests/numeric_source_price.test.mjs

.PHONY: all clean compact-probe subcommand-dispatch-test site-fragment-test import-jsonl-test fact-delta-test project-listings-owner-test publish-market-owner-test project-event-sources-test world-fragments-test install install-bin uninstall

# Page records, fragments and operation history share one owner transaction.
all: page-write-test
page-write-test: $(TARGET)
	bun test tests/page_write.test.mjs
.PHONY: page-write-test

all: source-publication-test
source-publication-test: $(TARGET)
	bun test tests/source_publication.test.mjs
.PHONY: source-publication-test

.PHONY: account-world-test
all: account-world-test
account-world-test: tests/account_world_test.c src/account_world.c src/account_world.h
	@set -eu; witness=$$(mktemp "$${TMPDIR:-/tmp}/account-world.XXXXXX"); \
	trap 'rm -f "$$witness"' 0 1 2 15; \
	$(CC) $(CFLAGS) -Werror tests/account_world_test.c src/account_world.c -lsqlite3 -o "$$witness"; "$$witness"
.PHONY: account-world-audio-test
all: account-world-audio-test
account-world-audio-test: tests/account_world_audio_test.c src/account_world.c src/account_world.h src/account_world_audio.c src/account_world_audio.h
	@set -eu; witness=$$(mktemp "$${TMPDIR:-/tmp}/account-world-audio.XXXXXX"); \
	trap 'rm -f "$$witness"' 0 1 2 15; \
	$(CC) $(CFLAGS) -Werror tests/account_world_audio_test.c src/account_world.c src/account_world_audio.c -lsqlite3 -o "$$witness"; "$$witness"
.PHONY: account-world-cinema-test
all: account-world-cinema-test
account-world-cinema-test: tests/account_world_cinema_test.c src/account_world.c src/account_world.h
	@set -eu; witness=$$(mktemp "$${TMPDIR:-/tmp}/account-world-cinema.XXXXXX"); \
	trap 'rm -f "$$witness"' 0 1 2 15; \
	$(CC) $(CFLAGS) -Werror tests/account_world_cinema_test.c src/account_world.c -lsqlite3 -o "$$witness"; "$$witness"
.PHONY: account-world-literature-test
all: account-world-literature-test
account-world-literature-test: tests/account_world_literature_test.c src/account_world.c src/account_world.h src/account_world_literature.c src/account_world_literature.h
	@set -eu; witness=$$(mktemp "$${TMPDIR:-/tmp}/account-world-literature.XXXXXX"); \
	trap 'rm -f "$$witness"' 0 1 2 15; \
	$(CC) $(CFLAGS) -Werror tests/account_world_literature_test.c src/account_world.c src/account_world_literature.c -lsqlite3 -o "$$witness"; "$$witness"

# Reference designs -> design_fingerprint records, at any daily volume.
#   make design-intake SOURCE=stitch-20261106 ZIPS="$HOME/Downloads/stitch_*.zip"
# Unzips each archive under $(DESIGN_ROOT)/$(SOURCE), measures every code.html
# with packs/aurekai/presentation/design-intake.sql (DuckDB) and imports the
# rows through the CMS owner in update mode, so a re-run refreshes rather than
# duplicates. Rows are unselected observations; promotion to a
# presentation_template or route_presentation row is a reviewed step.
DESIGN_ROOT ?= $(HOME)/.bonfyre/design-references
DESIGN_DB ?= $(HOME)/.bonfyre/estate-local/cms.db
.PHONY: design-intake
design-intake: $(TARGET)
	@test -n "$(SOURCE)" && test -n "$(ZIPS)" || { echo 'usage: make design-intake SOURCE=name ZIPS="a.zip b.zip"' >&2; exit 2; }
	@mkdir -p "$(DESIGN_ROOT)/$(SOURCE)"
	@for z in $(ZIPS); do d="$(DESIGN_ROOT)/$(SOURCE)/$$(basename "$$z" .zip | tr ' ()' '_--')"; mkdir -p "$$d"; unzip -o -q "$$z" -d "$$d"; done
	@set -e; out=$$(mktemp "$${TMPDIR:-/tmp}/design-intake.XXXXXX"); seen=$$(mktemp "$${TMPDIR:-/tmp}/design-seen.XXXXXX"); \
	 trap 'rm -f "$$out" "$$out.new" "$$seen"' 0 1 2 15; \
	 { echo "SET VARIABLE designs = '$(DESIGN_ROOT)/$(SOURCE)/*/*/*/code.html'; SET VARIABLE source = '$(SOURCE)';"; \
	   grep -v '^[[:space:]]*--' ../../packs/aurekai/presentation/design-intake.sql; } | duckdb -noheader -list > "$$out"; \
	 sqlite3 -readonly "$(DESIGN_DB)" "SELECT '\"fingerprint_id\":\"'||fingerprint_id||'\"' FROM design_fingerprint" > "$$seen" 2>/dev/null || :; \
	 grep -v -F -f "$$seen" "$$out" > "$$out.new" || :; \
	 echo "measured $$(wc -l < "$$out" | tr -d ' ') designs from $(SOURCE); $$(wc -l < "$$out.new" | tr -d ' ') new"; \
	 if [ -s "$$out.new" ]; then ./$(TARGET) entry import-jsonl design_fingerprint "$$out.new" --db "$(DESIGN_DB)" | tail -1; fi

# Where each measured design lands: candidate routes by pattern and whether its
# tokens already match the template those routes wear. Read-only.
#   make design-review SOURCE=stitch-20261106   -> $(DESIGN_ROOT)/$(SOURCE)/review.csv
.PHONY: design-review
design-review:
	@test -n "$(SOURCE)" || { echo 'usage: make design-review SOURCE=name' >&2; exit 2; }
	@mkdir -p "$(DESIGN_ROOT)/$(SOURCE)"
	@{ echo "SET VARIABLE source = '$(SOURCE)';"; \
	   grep -v '^[[:space:]]*--' ../../packs/aurekai/presentation/design-review.sql | sed "s#__CMS_DB__#$(DESIGN_DB)#"; } | \
	 duckdb -header -csv > "$(DESIGN_ROOT)/$(SOURCE)/review.csv"
	@echo "review queue: $(DESIGN_ROOT)/$(SOURCE)/review.csv ($$(($$(wc -l < "$(DESIGN_ROOT)/$(SOURCE)/review.csv") - 1)) designs)"
