.DEFAULT_GOAL := all
CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
ifeq ($(shell uname -s),Darwin)
# Expose no-follow descriptor traversal before system headers are parsed.
CFLAGS += -D_DARWIN_C_SOURCE
endif
DEAD_STRIP := -Wl,-dead_strip
ifeq ($(shell uname -s),Linux)
DEAD_STRIP := -Wl,--gc-sections
# glibc hides strcasestr, strptime and timegm behind _GNU_SOURCE once a source
# file asks for _POSIX_C_SOURCE. GCC's -Wall also reports brace-less one-line
# guards and possible snprintf truncation, which clang does not; the bounded
# writers check their own lengths.
CFLAGS += -D_GNU_SOURCE -Wno-misleading-indentation -Wno-format-truncation
endif
CFLAGS += -I../BonfyreLex/src
# Packed receipts are read where zstd is present; without it the organ reads
# loose receipts only, as it always did. Linked statically when the archive is there.
ZSTD_PREFIX ?= $(shell for p in /opt/homebrew /usr/local /opt/local /usr; do [ -f $$p/include/zstd.h ] && echo $$p && break; done)
ifneq ($(ZSTD_PREFIX),)
CFLAGS += -DBONFYRE_HAVE_ZSTD -I$(ZSTD_PREFIX)/include
ZSTD_LIBS = $(if $(wildcard $(ZSTD_PREFIX)/lib/libzstd.a),$(ZSTD_PREFIX)/lib/libzstd.a,-L$(ZSTD_PREFIX)/lib -lzstd)
endif
ROOT := ../..
# The system Zig cache can retain incomplete libcompiler_rt.a entries after disk
# pressure. Use an owned, separate derived cache for this test/build organ.
ZIG_GLOBAL_CACHE_DIR ?= $(HOME)/.bonfyre/cache/zig-web-address
export ZIG_GLOBAL_CACHE_DIR
TARGET = bonfyre-web-address
SITE_TARGET = bonfyre-site
# CommitmentGraph: a stated support and a name against a role are promises,
# and the promise rail is native rather than something to invent beside it.
CORE_SOURCES := $(ROOT)/engine/core/src/matrix.c $(ROOT)/engine/core/src/matrix_closure.c $(ROOT)/engine/core/src/authority.c \
                $(ROOT)/engine/core/src/commitment.c $(ROOT)/engine/core/src/commerce.c \
	$(ROOT)/engine/core/src/information_policy.c $(ROOT)/lib/libbonfyre/src/bf_sha256.c $(ROOT)/lib/libbonfyre/bf_json.c
PROJECTION_SOURCE := src/site_fragment_projection.c
PUBLIC_PROJECTION_CACHE_SOURCE := src/public_projection_cache.c
COMMUNITY_PROJECTION_SOURCE := src/community_projection.c
PUBLIC_HOME_SEARCH_SOURCE := src/public_home_search.c
TERRITORY_HOME_SOURCE := src/territory_home.c
SPATIAL_FRONTIER_SOURCE := src/spatial_frontier.c
SOURCE_HIERARCHY_SOURCE := src/source_hierarchy_projection.c
WEB_SOURCES := src/main.c src/public_code.c src/http_readiness.c src/atom_feed.c src/subject_address.c src/bounded_output.c $(SPATIAL_FRONTIER_SOURCE) $(PROJECTION_SOURCE) \
               src/source_depth_projection.c src/people_projection.c src/detail_depth.c src/model_catalogue_index.c \
               $(PUBLIC_PROJECTION_CACHE_SOURCE) $(COMMUNITY_PROJECTION_SOURCE) \
               $(TERRITORY_HOME_SOURCE) \
               src/domain_profile_projection.c $(SOURCE_HIERARCHY_SOURCE) src/site_product.c src/site_mcp.c src/site_presentation.c
CORE_NATIVE_SOURCES := $(filter $(ROOT)/engine/core/src/%.c,$(CORE_SOURCES))
WEB_OBJECTS := $(patsubst src/%.c,build/web_%.o,$(WEB_SOURCES))
CORE_OBJECTS := $(patsubst $(ROOT)/engine/core/src/%.c,build/core_%.o,$(CORE_NATIVE_SOURCES))
OBJECTS := $(WEB_OBJECTS) $(CORE_OBJECTS) build/lib_bf_sha256.o build/lib_bf_static_embed.o build/lib_bf_json.o build/lib_bf_jsonw.o build/lib_bf_msgpack.o build/recipe_material.o build/record_factor.o build/recipe_week.o build/address_resolve.o build/recipe_nutrition.o
OBJECTS += build/public_qr.o build/campaign_link.o build/lib_bf_utf8.o build/account_world.o
OBJECTS += build/account_world_audio.o
OBJECTS += build/account_world_literature.o
# These inputs are prepared explicitly once from verified retained bodies or
# the pinned upstream release. Normal builds never contact an external source.
.PHONY: public-code-inputs prepare-public-code
prepare-public-code:
	bun tools/prepare_public_code.mjs --fetch
public-code-inputs:
	bun tools/prepare_public_code.mjs --check
build/public_qr.o: build/public-code/qrcodegen.c build/public-code/qrcodegen.h | public-code-inputs
	$(CC) $(CFLAGS) -c build/public-code/qrcodegen.c -o $@
build/web_public_code.o: build/public-code/qrcodegen.h | public-code-inputs
build/public-code/qrcodegen.c build/public-code/qrcodegen.h:
	@echo 'Missing pinned QR source. Run make prepare-public-code or supply retained bodies to tools/prepare_public_code.mjs.' >&2; exit 1
BUILD_INCLUDES := -I$(ROOT)/engine/core/include -I$(ROOT)/lib/libbonfyre/include -Isrc -I../BonfyreRecipe/src -I../BonfyreCompress/src -I../BonfyreExternalEstate/src
SOURCE_HIERARCHY_TEST := build/source-hierarchy-projection-test
$(SOURCE_HIERARCHY_TEST): tests/source_hierarchy_projection_test.c $(SOURCE_HIERARCHY_SOURCE) src/source_hierarchy_projection.h $(ROOT)/lib/libbonfyre/include/bf_sqlite_path.h
	@mkdir -p build
	$(CC) $(CFLAGS) $(BUILD_INCLUDES) tests/source_hierarchy_projection_test.c $(SOURCE_HIERARCHY_SOURCE) -lsqlite3 -o $@
.PHONY: source-hierarchy-witness
all: native-link-footprint-witness source-hierarchy-witness
ifeq ($(shell uname -s),Darwin)
all: native-compact-footprint-witness
endif
test: source-hierarchy-witness
.PHONY: preview-snapshot-witness
# The preview copies live stores with APFS copy-on-write clones and refuses
# without them, so its witness runs on macOS.
ifeq ($(shell uname -s),Darwin)
test: preview-snapshot-witness
endif
preview-snapshot-witness:
	sh tools/preview-snapshot.test.sh
source-hierarchy-witness: $(SOURCE_HIERARCHY_TEST)
	./$(SOURCE_HIERARCHY_TEST)

# Exact read-only native owner witness. No WebAddress binary or remote release needed.
MODEL_CATALOGUE_INDEX_TEST := build/model-catalogue-index-test
$(MODEL_CATALOGUE_INDEX_TEST): tests/model_catalogue_index_test.c src/model_catalogue_index.c src/model_catalogue_index.h | build
	$(CC) $(CFLAGS) $(BUILD_INCLUDES) tests/model_catalogue_index_test.c src/model_catalogue_index.c -lsqlite3 -o $@
.PHONY: model-catalogue-index-witness
all test: model-catalogue-index-witness
model-catalogue-index-witness: $(MODEL_CATALOGUE_INDEX_TEST)
	./$(MODEL_CATALOGUE_INDEX_TEST)

SURFACE_PROGRAM_HEADER := generated/native_surface_programs.h
PRESENTATION_CELL_HEADER := generated/native_presentation_cells.h
MACHINE_SURFACE_HEADER := generated/native_machine_surfaces.h
EXPERIENCE_PROGRAM_HEADER := generated/native_experience_composition.h
$(EXPERIENCE_PROGRAM_HEADER): generated/native_page_composition.h ../BonfyreSurfaceScene/runtime/experience-programs.mjs ../BonfyreSurfaceScene/runtime/stitch-grammar.mjs tools/compile_experience_programs.mjs
	@bun tools/compile_experience_programs.mjs
build/web_main.o build/compact/web_main.o: $(EXPERIENCE_PROGRAM_HEADER)
.PHONY: native-experience-composition-witness
native-experience-composition-witness: $(EXPERIENCE_PROGRAM_HEADER)
	@bun tools/compile_experience_programs.mjs generated/native_page_composition.h $(EXPERIENCE_PROGRAM_HEADER) --check
TEMPLATE_SLOT_HEADER := generated/native_template_slots.h
PACK_BROWSER_HEADER := generated/native_pack_browser.h
$(PACK_BROWSER_HEADER): src/pack_browser.js src/pack_browser.css tools/compile_pack_browser.mjs
	bun tools/compile_pack_browser.mjs
build/web_main.o: $(PACK_BROWSER_HEADER)
build/web_main.o: src/item_credit_indexes.h
$(PEOPLE_PROJECTION_TEST): src/item_credit_indexes.h
build/web_main.o: src/front_recipe_query.h
build/web_main.o: src/static_asset_closure.h src/public_asset_export.h
build/web_main.o: src/account_activity_web.h src/account_world_web.h ../BonfyreCMS/src/account_world.h
build/account_world.o: ../BonfyreCMS/src/account_world.c ../BonfyreCMS/src/account_world.h | build
	$(CC) $(CFLAGS) -c ../BonfyreCMS/src/account_world.c -o $@
build/compact/account_world.o: ../BonfyreCMS/src/account_world.c ../BonfyreCMS/src/account_world.h | build/compact
	$(CC) $(COMPACT_FLAGS) -c ../BonfyreCMS/src/account_world.c -o $@
build/web_main.o build/compact/web_main.o: src/account_world_audio_web.h ../BonfyreCMS/src/account_world_audio.h
build/account_world_audio.o: ../BonfyreCMS/src/account_world_audio.c ../BonfyreCMS/src/account_world_audio.h ../BonfyreCMS/src/account_world.h | build
	$(CC) $(CFLAGS) -c ../BonfyreCMS/src/account_world_audio.c -o $@
build/compact/account_world_audio.o: ../BonfyreCMS/src/account_world_audio.c ../BonfyreCMS/src/account_world_audio.h ../BonfyreCMS/src/account_world.h | build/compact
	$(CC) $(COMPACT_FLAGS) -c ../BonfyreCMS/src/account_world_audio.c -o $@
build/web_main.o build/compact/web_main.o: src/account_world_literature_web.h ../BonfyreCMS/src/account_world_literature.h
build/account_world_literature.o: ../BonfyreCMS/src/account_world_literature.c ../BonfyreCMS/src/account_world_literature.h ../BonfyreCMS/src/account_world.h | build
	$(CC) $(CFLAGS) -c ../BonfyreCMS/src/account_world_literature.c -o $@
build/compact/account_world_literature.o: ../BonfyreCMS/src/account_world_literature.c ../BonfyreCMS/src/account_world_literature.h ../BonfyreCMS/src/account_world.h | build/compact
	$(CC) $(COMPACT_FLAGS) -c ../BonfyreCMS/src/account_world_literature.c -o $@

.PHONY: public-cache-contention-witness
all test: public-cache-contention-witness
public-cache-contention-witness: tests/public_projection_busy_test.c src/public_projection_cache.c
	@set -eu; witness=$$(mktemp "$${TMPDIR:-/tmp}/public-cache-test.XXXXXX"); \
	trap 'rm -f "$$witness"' 0 1 2 15; \
	$(CC) $(CFLAGS) $(BUILD_INCLUDES) tests/public_projection_busy_test.c src/public_projection_cache.c $(ROOT)/lib/libbonfyre/src/bf_sha256.c -lsqlite3 -o "$$witness"; "$$witness"

.PHONY: front-recipe-query-witness
all test: front-recipe-query-witness
front-recipe-query-witness: tests/front_recipe_query_test.c src/front_recipe_query.h
	@set -eu; witness=$$(mktemp "$${TMPDIR:-/tmp}/front-recipe-test.XXXXXX"); \
	trap 'rm -f "$$witness"' 0 1 2 15; \
	$(CC) $(CFLAGS) tests/front_recipe_query_test.c -lsqlite3 -o "$$witness"; "$$witness"
build/web_main.o: src/community_journey.h ../BonfyreCMS/src/community_read.h

.PHONY: static-asset-closure-witness
all test: static-asset-closure-witness
static-asset-closure-witness: tests/static_asset_closure_test.c src/static_asset_closure.h src/public_asset_export.h
	@set -eu; witness=$$(mktemp "$${TMPDIR:-/tmp}/public-assets-test.XXXXXX"); \
	trap 'rm -f "$$witness"' 0 1 2 15; \
	$(CC) $(CFLAGS) -Wno-unused-function tests/static_asset_closure_test.c -o "$$witness"; "$$witness"
build/web_main.o: src/application_workspace.h src/procurement_approval_status.h src/procurement_enactment.h src/procurement_enactment_effect.h
build/web_main.o: src/campaign_workspace.h src/native_publications_query.h src/relationship_observation.h
build/campaign_link.o: ../BonfyreCampaign/src/campaign.c ../BonfyreCampaign/src/campaign.h | build
	$(CC) $(CFLAGS) $(BUILD_INCLUDES) -MMD -MP -c -o $@ $<
build/lib_bf_utf8.o: $(ROOT)/lib/libbonfyre/bf_utf8.c | build
	$(CC) $(CFLAGS) $(BUILD_INCLUDES) -MMD -MP -c -o $@ $<
.PHONY: crm-finance-witness
all test: crm-finance-witness
crm-finance-witness: $(TARGET)
	bun test ../../components/BonfyreServiceSite/tests/workbook-native.test.mjs
.PHONY: campaign-workspace-witness
test: campaign-workspace-witness
campaign-workspace-witness:
	$(MAKE) -f tests/campaign_workspace_test.mk test
.PHONY: native-publications-query-witness
test: native-publications-query-witness
native-publications-query-witness:
	$(MAKE) -f tests/native_publications_query_test.mk test
.PHONY: publisher-front-witness
test: publisher-front-witness
publisher-front-witness: $(TARGET)
	sh tests/publisher_front_archive_test.sh $(abspath $(TARGET))
	sh tests/front_sheets_served_test.sh $(abspath $(TARGET))
	sh tests/wire_cache_law_test.sh $(abspath $(TARGET))

.PHONY: procurement-approval-status-witness
test: procurement-approval-status-witness
procurement-approval-status-witness: $(TARGET)
	bun test tests/procurement_approval_status.test.mjs tests/procurement_enactment_readiness.test.mjs tests/procurement_enactment_effect.test.mjs

# Compiled glyphs, declared licences and product-route semantics share one witness.
.PHONY: glyph-vocabulary-witness
all test: glyph-vocabulary-witness
glyph-vocabulary-witness:
	sh tests/glyph_set_test.sh

.PHONY: work-provenance-witness
test: work-provenance-witness
work-provenance-witness: $(TARGET)
	sh tests/work_provenance_test.sh $(abspath $(TARGET))

.PHONY: account-activity-witness
all: account-activity-witness
test: account-activity-witness
account-activity-witness: $(TARGET)
	$(MAKE) -C ../BonfyreQueue bonfyre-queue
	@mkdir -p build
	@BONFYRE_TEST_QUEUE="$(abspath ../BonfyreQueue/bonfyre-queue)" bun test tests/account_activity_web.test.mjs tests/account_inbox_web.test.mjs ../../integrations/browser/tests/aurekai-inbox-source.test.mjs tests/account_activity_chrome.test.mjs ../../integrations/browser/tests/aurekai-activity-interaction.browser.test.mjs >build/account-activity-witness.log 2>&1; result=$$?; \
	tail -n 80 build/account-activity-witness.log; exit $$result
	$(MAKE) -C ../BonfyreQueue

.PHONY: enrollment-continuation-witness
all: enrollment-continuation-witness
test: enrollment-continuation-witness
enrollment-continuation-witness:
	$(MAKE) -C ../../integrations/browser test-enrollment-continuation

.PHONY: program-workspace-witness
all: program-workspace-witness
test: program-workspace-witness
program-workspace-witness:
	$(MAKE) -C ../../integrations/browser test-programs

.PHONY: public-preview-diversity-witness
test: public-preview-diversity-witness
public-preview-diversity-witness:
	bun test tests/page_preview_diversity.test.mjs

# Optional Playwright runner; the default browser witness uses installed Chrome.
.PHONY: account-activity-playwright
account-activity-playwright: $(TARGET)
	bun test \
	../../integrations/browser/tests/aurekai-activity.browser.test.mjs \
	../../integrations/browser/tests/aurekai-activity-interaction.browser.test.mjs

.PHONY: participant-return-witness
test: participant-return-witness
participant-return-witness: $(TARGET)
	$(MAKE) -C ../BonfyreCMS bonfyre-cms
	BONFYRE_TEST_WEB_BINARY="$(or $(BONFYRE_TEST_WEB_BINARY),$(CURDIR)/$(TARGET))" \
	BONFYRE_TEST_CMS_BINARY="$(or $(BONFYRE_TEST_CMS_BINARY),$(abspath ../BonfyreCMS/bonfyre-cms))" \
	bun test tests/participant_journey_browser.test.mjs
# The signed-in profile is an account working set, not only a published site.
all: community-journey-witness
.PHONY: community-journey-witness
test: community-journey-witness
community-journey-witness: $(TARGET)
	$(MAKE) -C ../BonfyreCMS bonfyre-cms
	BONFYRE_TEST_WEB_BINARY="$${BONFYRE_TEST_WEB_BINARY:-$$(pwd)/$(TARGET)}" \
	BONFYRE_TEST_CMS_BINARY="$${BONFYRE_TEST_CMS_BINARY:-$$(cd ../BonfyreCMS && pwd)/bonfyre-cms}" \
	bun test tests/community_journey.test.mjs tests/profile_site_scope.test.mjs
.PHONY: front-arrival-witness
all: front-arrival-witness
test: front-arrival-witness
front-arrival-witness: $(TARGET)
	$(MAKE) -C ../BonfyreCMS bonfyre-cms
	sh tests/front_arrival_test.sh ./$(TARGET)
.PHONY: pack-browser-witness
test: pack-browser-witness
pack-browser-witness: $(TARGET) $(PACK_BROWSER_HEADER)
	bun tools/compile_pack_browser.mjs --check
	bun test tests/pack_browser_test.mjs
DOMAIN_PROFILE_HEADER := generated/native_domain_profiles.h
include tools/domain_profiles.mk
SITE_APPLICATION_LOWERER_HEADER := generated/native_site_application_lowerers.h
WORLD_MATERIAL_RENDERER_HEADER := generated/native_world_material_renderers.h
PAGE_COMPOSITION_HEADER := generated/native_page_composition.h
PAGE_COMPOSITION_JSON := generated/native-page-composition.json
AUDIENCE_PRESENTATION_HEADER := generated/native_audience_presentations.h
# Every generated contract, named as a prerequisite rather than nine of the
# twelve. The governance grammar, the institution planes and the operator
# affinities were each compiled from CUE and then not listed here, so changing
# one left the binary untouched: the contract moved and the running server did
# not. A generated header nothing depends on gates nothing.
GENERATED_HEADERS := $(sort $(filter-out %.verify.h,$(wildcard generated/native_*.h)) $(DOMAIN_PROFILE_HEADER))
PROJECTION_TEST := tests/site-fragment-projection-test
TERRITORY_HOME_TEST := tests/territory-home-test
PUBLIC_HOME_SEARCH_TEST := tests/public-home-search-test
DOMAIN_PROFILE_TEST := tests/domain-profile-projection-test
PEOPLE_PROJECTION_TEST := tests/people-projection-test
ATOM_FEED_TEST := tests/atom-feed-test
SUBJECT_ADDRESS_TEST := tests/subject-address-test
BOUNDED_OUTPUT_TEST := tests/bounded-output-test
PEOPLE_STATIC_MANIFEST := tools/people-static-manifest
HOME_SURFACE := $(ROOT)/site/matrix/surfaces/home.html
SURFACE_RUNTIME_SOURCE := ../BonfyreSurfaceScene/runtime/surface-runtime.mjs
SURFACE_RUNTIME_ASSET := $(ROOT)/site/matrix/bonfyre-surface-runtime.mjs
WORKSPACE_COMPOSITION_SOURCE := ../BonfyreSurfaceScene/runtime/renderers/workspace-composition.mjs
WORKSPACE_COMPOSITION_ASSET := $(ROOT)/site/matrix/bonfyre-workspace-composition.mjs
# Derived icon cuts are made from the existing knight PNG by macOS sips.
KNIGHT_SOURCE := $(ROOT)/site/matrix/aurekai-knight.png
KNIGHT_SIZES := 16 32 48 64 180
KNIGHT_ICONS := $(foreach n,$(KNIGHT_SIZES),$(ROOT)/site/matrix/aurekai-knight-$(n).png)
ifneq ($(shell command -v sips 2>/dev/null),)
all: knight-icons
endif
.PHONY: knight-icons
knight-icons: $(KNIGHT_ICONS)
$(ROOT)/site/matrix/aurekai-knight-%.png: $(KNIGHT_SOURCE)
	@sips -s format png -z $* $* "$<" --out "$@" >/dev/null
	@test -s "$@"

.PHONY: all clean test static-audit static-audit-check static-audit-detail household-plane-witness clone-grammar-witness model-commons-witness model-commons-witness projection-witness territory-home-witness surface-pattern-witness foundry-behavior-witness watch-media-witness page-programs-witness page-events-witness page-stage-witness surface-home surface-runtime-asset page-surface-scene-witness condition-words-held-desk-witness page-composition-json
all: page-composition-json surface-home surface-runtime-asset $(TARGET) $(SITE_TARGET) $(PEOPLE_STATIC_MANIFEST) household-plane-witness clone-grammar-witness model-commons-witness projection-witness territory-home-witness surface-pattern-witness watch-media-witness page-programs-witness page-stage-witness page-surface-scene-witness condition-words-held-desk-witness
.PHONY: workspace-material-witness
all: workspace-material-witness
workspace-material-witness:
	node --test tests/workspace_material.test.mjs
	bun test ../../integrations/browser/tests/native-workbench-package.test.mjs
.PHONY: foreign-workbench-routes-witness
all: foreign-workbench-routes-witness
foreign-workbench-routes-witness: $(TARGET)
	sh ./tests/native_surface_routes_test.sh ./$(TARGET)
.PHONY: discovery-review-route-witness
all: discovery-review-route-witness
discovery-review-route-witness: $(TARGET)
	$(MAKE) -C ../BonfyreEconomy bonfyre-economy
	sh ./tests/discovery_review_route_test.sh ./$(TARGET)
static-audit:
	@for rule in analysis/*.yml; do \
		printf '%s: ' "$$rule"; \
		ast-grep scan --rule "$$rule" --json=stream src/main.c | wc -l; \
	done
static-audit-check:
	ast-grep scan --rule analysis/clean-compiler-offset.yml --report-style short src/main.c
static-audit-detail:
	@for rule in analysis/*.yml; do ast-grep scan --rule "$$rule" --report-style short src/main.c; done
page-composition-json: $(PAGE_COMPOSITION_JSON)
$(PAGE_COMPOSITION_JSON): $(wildcard $(ROOT)/schemas/control/*.cue)
	@mkdir -p generated
	@cd $(ROOT)/schemas/control && cue export . -e nativePageComposition --out json > ../../cmd/BonfyreWebAddress/$(PAGE_COMPOSITION_JSON)
surface-runtime-asset: $(SURFACE_RUNTIME_SOURCE) $(WORKSPACE_COMPOSITION_SOURCE)
	@cmp -s "$(SURFACE_RUNTIME_SOURCE)" "$(SURFACE_RUNTIME_ASSET)" 2>/dev/null || cp "$(SURFACE_RUNTIME_SOURCE)" "$(SURFACE_RUNTIME_ASSET)"
	@cmp -s "$(SURFACE_RUNTIME_SOURCE)" "$(SURFACE_RUNTIME_ASSET)"
	@cmp -s "$(WORKSPACE_COMPOSITION_SOURCE)" "$(WORKSPACE_COMPOSITION_ASSET)" 2>/dev/null || cp "$(WORKSPACE_COMPOSITION_SOURCE)" "$(WORKSPACE_COMPOSITION_ASSET)"
	@cmp -s "$(WORKSPACE_COMPOSITION_SOURCE)" "$(WORKSPACE_COMPOSITION_ASSET)"
page-surface-scene-witness: surface-runtime-asset $(TARGET)
	./tests/page_surface_scene_test.sh ./$(TARGET)
condition-words-held-desk-witness: $(TARGET)
	./tests/condition_words_held_desk_test.sh ./$(TARGET)
surface-home:
	@test -s "$(HOME_SURFACE)"
	@grep -q '<!--BF_TEMPLATE_SLOT:0-->' "$(HOME_SURFACE)"
	@grep -q '<!--BF_TEMPLATE_SLOT:1-->' "$(HOME_SURFACE)"
	@grep -q 'action="/_surface/intake"' "$(HOME_SURFACE)"
	@grep -q 'Prepare route' "$(HOME_SURFACE)"
	@grep -q 'id="territory"' "$(HOME_SURFACE)"
	@grep -q 'hx-get="/_surface/home-weather"' "$(HOME_SURFACE)"
	@grep -q 'id="estate-try"' "$(HOME_SURFACE)"
	@grep -q 'hx-get="/_surface/external-estate"' "$(HOME_SURFACE)"
	@! grep -q 'bf_apply_public_home_search' src/main.c
	@! grep -q 'location.hostname' "$(HOME_SURFACE)"
	@! grep -q 'fetch("/foreign/weather"' "$(HOME_SURFACE)"
	@! grep -q '/external-estate/' "$(HOME_SURFACE)"
	@! grep -q "localStorage.getItem('bonfyre_token')" "$(HOME_SURFACE)"
$(TARGET): $(OBJECTS)
	$(CC) $(CFLAGS) $(DEAD_STRIP) -o $@ $(OBJECTS) -lsqlite3 -lm -lz $(ZSTD_LIBS)

# Size experiment only: link the same verified objects without static zstd.
# A smaller file is not a release without an attested runtime and full tests.
.PHONY: native-link-footprint-witness
native-link-footprint-witness: $(TARGET) build/bonfyre-web-address-link-candidate
	@wc -c $(TARGET) build/bonfyre-web-address-link-candidate
build/bonfyre-web-address-link-candidate: $(OBJECTS)
	$(CC) $(CFLAGS) $(DEAD_STRIP) -o $@ $(OBJECTS) -lsqlite3 -lm -lz $(if $(ZSTD_PREFIX),-L$(ZSTD_PREFIX)/lib -lzstd,)

# Reproducible size-first objects from current source, never installed.
COMPACT_FLAGS = $(CFLAGS) -Oz -flto=full -fno-unwind-tables -fno-asynchronous-unwind-tables -fvisibility=hidden
COMPACT_OBJECTS = $(patsubst build/%,build/compact/%,$(OBJECTS))
$(COMPACT_OBJECTS): build/compact/.full-lto-hidden-verified
build/compact/.full-lto-hidden-verified: | build/compact
	@touch $@
.PHONY: native-compact-footprint-witness
native-compact-footprint-witness: $(TARGET) build/bonfyre-web-address-compact-stripped
	@wc -c $(TARGET) build/bonfyre-web-address-compact build/bonfyre-web-address-compact-stripped
.PHONY: native-compact-product-witness
# The compact product is a full-LTO Mach-O build (-exported_symbol); macOS only.
ifeq ($(shell uname -s),Darwin)
all test: native-compact-product-witness
endif
native-compact-product-witness: build/bonfyre-web-address-compact-stripped
	@echo "Testing size-first WebAddress against the isolated site-product contract"
	sh tests/site_product_test.sh ./build/bonfyre-web-address-compact-stripped
build/bonfyre-web-address-compact-stripped: build/bonfyre-web-address-compact
	cp $< $@
	strip -x -S $@
build/bonfyre-web-address-compact: $(COMPACT_OBJECTS) Makefile
	$(CC) $(COMPACT_FLAGS) -Wl,-dead_strip -Wl,-exported_symbol,_main -o $@ $(COMPACT_OBJECTS) -lsqlite3 -lm -lz $(ZSTD_LIBS)
build/compact:
	mkdir -p $@
build/compact/web_main.o: $(GENERATED_HEADERS) src/account_world_web.h ../BonfyreCMS/src/account_world.h
build/compact/web_domain_profile_projection.o: $(DOMAIN_PROFILE_HEADER)
build/compact/web_public_code.o: build/public-code/qrcodegen.h | public-code-inputs
build/compact/public_qr.o: build/public-code/qrcodegen.c build/public-code/qrcodegen.h | build/compact public-code-inputs
	$(CC) $(COMPACT_FLAGS) -c build/public-code/qrcodegen.c -o $@
build/compact/recipe_material.o: ../BonfyreRecipe/src/recipe_material.c | build/compact
	$(CC) $(COMPACT_FLAGS) $(BUILD_INCLUDES) -MMD -MP -c -o $@ $<
build/compact/recipe_week.o: ../BonfyreRecipe/src/recipe_week.c | build/compact
	$(CC) $(COMPACT_FLAGS) $(BUILD_INCLUDES) -MMD -MP -c -o $@ $<
build/compact/recipe_nutrition.o: ../BonfyreRecipe/src/recipe_nutrition.c | build/compact
	$(CC) $(COMPACT_FLAGS) $(BUILD_INCLUDES) -MMD -MP -c -o $@ $<
build/compact/address_resolve.o: ../BonfyreExternalEstate/src/address_resolve.c | build/compact
	$(CC) $(COMPACT_FLAGS) $(BUILD_INCLUDES) -MMD -MP -c -o $@ $<
build/compact/record_factor.o: ../BonfyreCompress/src/record_factor.c | build/compact
	$(CC) $(COMPACT_FLAGS) $(BUILD_INCLUDES) -MMD -MP -c -o $@ $<
build/compact/campaign_link.o: ../BonfyreCampaign/src/campaign.c | build/compact
	$(CC) $(COMPACT_FLAGS) $(BUILD_INCLUDES) -MMD -MP -c -o $@ $<
build/compact/lib_bf_utf8.o: $(ROOT)/lib/libbonfyre/bf_utf8.c | build/compact
	$(CC) $(COMPACT_FLAGS) $(BUILD_INCLUDES) -MMD -MP -c -o $@ $<
build/compact/lib_bf_%.o: $(ROOT)/lib/libbonfyre/src/bf_%.c | build/compact
	$(CC) $(COMPACT_FLAGS) $(BUILD_INCLUDES) -MMD -MP -c -o $@ $<
build/compact/lib_bf_json.o: $(ROOT)/lib/libbonfyre/bf_json.c | build/compact
	$(CC) $(COMPACT_FLAGS) $(BUILD_INCLUDES) -MMD -MP -c -o $@ $<
build/compact/lib_bf_jsonw.o: $(ROOT)/lib/libbonfyre/bf_jsonw.c | build/compact
	$(CC) $(COMPACT_FLAGS) $(BUILD_INCLUDES) -MMD -MP -c -o $@ $<
build/compact/lib_bf_msgpack.o: $(ROOT)/lib/libbonfyre/bf_msgpack.c | build/compact
	$(CC) $(COMPACT_FLAGS) $(BUILD_INCLUDES) -MMD -MP -c -o $@ $<
build/compact/web_%.o: src/%.c | build/compact
	$(CC) $(COMPACT_FLAGS) $(BUILD_INCLUDES) -MMD -MP -c -o $@ $<
build/compact/core_%.o: $(ROOT)/engine/core/src/%.c | build/compact
	$(CC) $(COMPACT_FLAGS) $(BUILD_INCLUDES) -MMD -MP -c -o $@ $<

# The CLI resolves this product name before looking in the installed runtime.
# Keep it attached to the resolver, including when replacing an older copy.
.PHONY: $(SITE_TARGET) site-product-witness
$(SITE_TARGET): $(TARGET)
	ln -sf $(TARGET) $@

.PHONY: frozen-site-alias-witness
all: frozen-site-alias-witness
frozen-site-alias-witness:
	bun test ../../scripts/release/tests/public_surface_rollout.test.mjs --test-name-pattern "frozen site uses"
	bun test ../../scripts/release/tests/public_surface_rollout.test.mjs --test-name-pattern "frozen clone inputs"
	bun test ../../scripts/release/tests/public_surface_rollout.test.mjs --test-name-pattern "frozen CUE runtime"

.PHONY: service-site-release-witness
all test: service-site-release-witness
service-site-release-witness:
	bun test ../../components/BonfyreServiceSite/tests/routing.test.mjs
.PHONY: browser-extension-parity-witness
all test: browser-extension-parity-witness
browser-extension-parity-witness:
	bun ../../scripts/project-recurring-commerce-browser.mjs --check
	bun ../../scripts/project-provider-work-browser.mjs --check


site-product-witness: $(SITE_TARGET)
	test -L $(SITE_TARGET)
	test "$$(readlink $(SITE_TARGET))" = "$(TARGET)"
	bun test tests/site_page_replace.test.mjs
	sh tests/site_product_test.sh ./$(TARGET)
	sh tests/site_offer_intake_test.sh ./$(TARGET)
	sh ../BonfyreBrief/tests/practice_site_test.sh ./$(TARGET)
	sh tests/site_search_test.sh ./$(TARGET)
build:
	mkdir -p $@
build/web_main.o: $(GENERATED_HEADERS)
build/web_domain_profile_projection.o: $(DOMAIN_PROFILE_HEADER)
build/recipe_material.o: ../BonfyreRecipe/src/recipe_material.c ../BonfyreRecipe/src/recipe_material.h ../BonfyreCompress/src/record_factor.h | build
	$(CC) $(CFLAGS) $(BUILD_INCLUDES) -MMD -MP -c -o $@ $<
build/recipe_week.o: ../BonfyreRecipe/src/recipe_week.c ../BonfyreRecipe/src/recipe_week.h ../BonfyreRecipe/src/recipe_material.h | build
	$(CC) $(CFLAGS) $(BUILD_INCLUDES) -MMD -MP -c -o $@ $<
build/recipe_nutrition.o: ../BonfyreRecipe/src/recipe_nutrition.c ../BonfyreRecipe/src/recipe_nutrition.h | build
	$(CC) $(CFLAGS) $(BUILD_INCLUDES) -MMD -MP -c -o $@ $<
build/address_resolve.o: ../BonfyreExternalEstate/src/address_resolve.c ../BonfyreExternalEstate/src/address_resolve.h | build
	$(CC) $(CFLAGS) $(BUILD_INCLUDES) -MMD -MP -c -o $@ $<
build/record_factor.o: ../BonfyreCompress/src/record_factor.c ../BonfyreCompress/src/record_factor.h | build
	$(CC) $(CFLAGS) $(BUILD_INCLUDES) -MMD -MP -c -o $@ $<
build/web_%.o: src/%.c | build
	$(CC) $(CFLAGS) $(BUILD_INCLUDES) -MMD -MP -c -o $@ $<
build/core_%.o: $(ROOT)/engine/core/src/%.c | build
	$(CC) $(CFLAGS) $(BUILD_INCLUDES) -MMD -MP -c -o $@ $<
build/lib_bf_sha256.o: $(ROOT)/lib/libbonfyre/src/bf_sha256.c | build
	$(CC) $(CFLAGS) $(BUILD_INCLUDES) -MMD -MP -c -o $@ $<
build/lib_bf_static_embed.o: $(ROOT)/lib/libbonfyre/src/bf_static_embed.c $(ROOT)/lib/libbonfyre/include/bf_static_embed.h | build
	$(CC) $(CFLAGS) $(BUILD_INCLUDES) -MMD -MP -c -o $@ $<
build/lib_bf_json.o: $(ROOT)/lib/libbonfyre/bf_json.c | build
	$(CC) $(CFLAGS) $(BUILD_INCLUDES) -MMD -MP -c -o $@ $<
build/lib_bf_jsonw.o: $(ROOT)/lib/libbonfyre/bf_jsonw.c | build
	$(CC) $(CFLAGS) $(BUILD_INCLUDES) -MMD -MP -c -o $@ $<
build/lib_bf_msgpack.o: $(ROOT)/lib/libbonfyre/bf_msgpack.c | build
	$(CC) $(CFLAGS) $(BUILD_INCLUDES) -MMD -MP -c -o $@ $<
-include $(OBJECTS:.o=.d)
$(PROJECTION_TEST): tests/site_fragment_projection_test.c $(PROJECTION_SOURCE) src/site_fragment_projection.h
	$(CC) $(CFLAGS) -Isrc -o $@ tests/site_fragment_projection_test.c $(PROJECTION_SOURCE) -lsqlite3
$(ATOM_FEED_TEST): tests/atom_feed_test.c src/atom_feed.c src/atom_feed.h $(ROOT)/lib/libbonfyre/src/bf_sha256.c
	$(CC) $(CFLAGS) $(BUILD_INCLUDES) -o $@ tests/atom_feed_test.c src/atom_feed.c $(ROOT)/lib/libbonfyre/src/bf_sha256.c -lsqlite3
.PHONY: atom-feed-witness
atom-feed-witness: $(ATOM_FEED_TEST)
	./$(ATOM_FEED_TEST)
.PHONY: sqlite-path-witness
sqlite-path-witness: | build
	$(CC) $(CFLAGS) $(BUILD_INCLUDES) -o build/sqlite_path_test tests/sqlite_path_test.c -lsqlite3
	./build/sqlite_path_test
$(SUBJECT_ADDRESS_TEST): tests/subject_address_test.c src/subject_address.c src/subject_address.h
	$(CC) $(CFLAGS) $(BUILD_INCLUDES) -o $@ tests/subject_address_test.c src/subject_address.c -lsqlite3
.PHONY: subject-address-witness
subject-address-witness: $(SUBJECT_ADDRESS_TEST)
	./$(SUBJECT_ADDRESS_TEST)
projection-witness: $(PROJECTION_TEST)
	./$(PROJECTION_TEST)
$(TERRITORY_HOME_TEST): tests/territory_home_test.c $(TERRITORY_HOME_SOURCE) src/territory_home.h
	$(CC) $(CFLAGS) -Isrc -o $@ tests/territory_home_test.c $(TERRITORY_HOME_SOURCE) src/site_presentation.c -lsqlite3
territory-home-witness: $(TERRITORY_HOME_TEST)
	./$(TERRITORY_HOME_TEST)
household-plane-witness: $(TARGET)
	./tests/household_plane_test.sh ./$(TARGET)

clone-grammar-witness: $(TARGET)
	./tests/clone_grammar_test.sh ./$(TARGET)

model-commons-witness: $(TARGET)
	./tests/model_commons_test.sh ./$(TARGET)

surface-pattern-witness: $(TARGET)
	./tests/surface_pattern_contract_test.sh ./$(TARGET)
foundry-behavior-witness: $(TARGET)
	bun test ../BonfyreCMS/tools/compile_foundry_behavior.test.mjs ../BonfyreCMS/tools/compile_role_program.test.mjs
	sh ./tests/foundry_behavior_owner_test.sh ./$(TARGET)
	sh ./tests/github_foundry_closure_test.sh ./$(TARGET)
watch-media-witness:
	sh ./tests/watch_media_program_test.sh
page-programs-witness: $(TARGET)
page-programs-witness: foreign-workbench-routes-witness
	./tests/page_programs_test.sh ./$(TARGET)
	sh ./tests/compiled_subject_route_web_test.sh ./$(TARGET)
page-events-witness: $(TARGET)
	./tests/page_events_test.sh ./$(TARGET)
	./tests/page_occurrence_event_test.sh ./$(TARGET)
	./tests/page_transport_test.sh ./$(TARGET)
page-stage-witness: $(TARGET)
	./tests/page_stage_test.sh ./$(TARGET)
surface-programs-verify:
	@mkdir -p generated
	@cue export ../../schemas/control -e nativeSurfaceProgramCompilerInput --out json > generated/native-surface-programs.verify.json
	@zig run tools/compile_surface_programs.zig -- generated/native-surface-programs.verify.json generated/native_surface_programs.verify.h
	@cmp -s generated/native_surface_programs.h generated/native_surface_programs.verify.h
	@grep -q 'bf_surface_renderer_presentations' generated/native_surface_programs.h
	@grep -q 'BF_SURFACE_DATE' generated/native_surface_programs.h
	@grep -q 'world_route' generated/native_surface_programs.h
	@grep -q 'bf_surface_territory_primary' generated/native_surface_programs.h
	@! grep -q 'ctx->renderer\|program->renderer' src/main.c
	@! sed -n '/static int serve_native_donor_route/,/static int studio_safe_slug_arg/p' src/main.c | grep -q 'target, "/discover"\|target, "/stage"\|target, "/community"\|target, "/board"\|target, "/listings"'
	@! grep -q 'Published views")) cut' src/main.c
	@! sed -n '/static const char \*native_night_primary/,/static void native_night_date/p' src/main.c | grep -q 'territory, "shop"\|territory, "work"\|territory, "live"'
	@rm -f generated/native-surface-programs.verify.json generated/native_surface_programs.verify.h
	@cue export ../../schemas/control -e surfacePresentationCellSet --out json > generated/native-presentation-cells.verify.json
	@zig run tools/compile_presentation_cells.zig -- generated/native-presentation-cells.verify.json generated/native_presentation_cells.verify.h
	@cmp -s generated/native_presentation_cells.h generated/native_presentation_cells.verify.h
	@grep -q 'BF_NATIVE_PRESENTATION_CELL_COUNT' generated/native_presentation_cells.h
	@grep -q '"discover","anonymous","What lawful possibilities intersect this addressed situation?' generated/native_presentation_cells.h
	@grep -q 'bf_presentation_discover_anonymous_roles' generated/native_presentation_cells.h
	@grep -q 'bf_presentation_discover_anonymous_requires' generated/native_presentation_cells.h
	@! rg -n 'ignore_unknown_fields\s*=\s*true' tools/compile_*.zig
	@! grep -q 'composition_renderer_family(\|composition_renderer_variant(\|composition_extent(\|composition_density(' src/main.c
	@! sed -n '/static const char \*world_grammar_route/,/static const char \*world_nested_grammar_route/p' src/main.c | grep -q 'static const struct.*route\|"discover", "/discover"'
	@rm -f generated/native-presentation-cells.verify.json generated/native_presentation_cells.verify.h
	@cue export ../../schemas/control -e nativeMachineSurfaces --out json > generated/native-machine-surfaces.verify.json
	@zig run tools/compile_machine_surfaces.zig -- generated/native-machine-surfaces.verify.json generated/native_machine_surfaces.verify.h
	@cmp -s generated/native_machine_surfaces.h generated/native_machine_surfaces.verify.h
	@rm -f generated/native-machine-surfaces.verify.json generated/native_machine_surfaces.verify.h
	@cue export ../../schemas/control -e nativeTemplateSlots --out json > generated/native-template-slots.verify.json
	@zig run tools/compile_template_slots.zig -- generated/native-template-slots.verify.json generated/native_template_slots.verify.h
	@cmp -s generated/native_template_slots.h generated/native_template_slots.verify.h
	@rm -f generated/native-template-slots.verify.json generated/native_template_slots.verify.h
	@cd $(ROOT)/schemas/control && cue export . -e nativeDomainProfiles --out json > ../../cmd/BonfyreWebAddress/generated/native-domain-profiles.verify.json
	@zig run tools/compile_domain_profiles.zig -- generated/native-domain-profiles.verify.json generated/native_domain_profiles.verify.h
	@cmp -s generated/native_domain_profiles.h generated/native_domain_profiles.verify.h
	@rm -f generated/native-domain-profiles.verify.json generated/native_domain_profiles.verify.h
	@cd $(ROOT)/schemas/control && cue export . -e nativeSiteApplicationLowerers --out json > ../../cmd/BonfyreWebAddress/generated/native-site-application-lowerers.verify.json
	@zig run tools/compile_site_application_lowerers.zig -- generated/native-site-application-lowerers.verify.json generated/native_site_application_lowerers.verify.h
	@cmp -s generated/native_site_application_lowerers.h generated/native_site_application_lowerers.verify.h
	@grep -q 'PublicationOccurrence' generated/native_site_application_lowerers.h
	@grep -q 'presentation_class' generated/native_site_application_lowerers.h
	@grep -q 'query_key' generated/native_site_application_lowerers.h
	@grep -q 'BF_SITE_MATERIAL_READER_WORLD_FRAGMENT' generated/native_site_application_lowerers.h
	@grep -q 'BF_SITE_OP_HEADER' generated/native_site_application_lowerers.h
	@grep -q 'bf_site_application_Watch_ops' generated/native_site_application_lowerers.h
	@! grep -q 'application_kind.*PublicationFeed\|application_kind.*Weather' src/main.c
	@! grep -q 'switch (lowerer->layout)' src/main.c
	@! sed -n '/static int site_material_resolve/,/static void site_surface_link/p' src/main.c | grep -q 'strcmp(lowerer->material_owner'
	@! rg -n 'site_program_emit_(publication_river|community_thread|listen_program|watch_program)' src/main.c
	@! sed -n '/static int serve_site_program/,/static int serve_world_fragments/p' src/main.c | grep -q 'json_extract(b.material_query'
	@! sed -n '/static int serve_site_program/,/static int serve_world_fragments/p' src/main.c | grep -Fq -e '$$.community' -e '$$.recording' -e '$$.program'
	@rm -f generated/native-site-application-lowerers.verify.json generated/native_site_application_lowerers.verify.h
	@cd $(ROOT)/schemas/control && cue export . -e nativeWorldMaterialRenderers --out json > ../../cmd/BonfyreWebAddress/generated/native-world-material-renderers.verify.json
	@zig run tools/compile_world_material_renderers.zig -- generated/native-world-material-renderers.verify.json generated/native_world_material_renderers.verify.h
	@cmp -s generated/native_world_material_renderers.h generated/native_world_material_renderers.verify.h
	@grep -q 'BF_WORLD_MATERIAL_PIN' generated/native_world_material_renderers.h
	@! sed -n '/static void world_material_open/,/static void world_material_close/p' src/main.c | grep -q 'strcmp(family'
	@rm -f generated/native-world-material-renderers.verify.json generated/native_world_material_renderers.verify.h
	@cd $(ROOT)/schemas/control && cue export . -e nativeCellContracts --out json > ../../cmd/BonfyreWebAddress/generated/native-cell-contracts.verify.json
	@cd $(ROOT)/schemas/control && cue export . -e nativePathChanges --out json > ../../cmd/BonfyreWebAddress/generated/native-path-changes.verify.json
	@cd $(ROOT)/schemas/control && cue export . -e nativeMessageStates --out json > ../../cmd/BonfyreWebAddress/generated/native-message-states.verify.json
	@zig run tools/compile_missing_cells.zig -- generated/native-cell-contracts.verify.json generated/native-path-changes.verify.json generated/native-message-states.verify.json generated/native_missing_cells.verify.h
	@cmp -s generated/native_missing_cells.h generated/native_missing_cells.verify.h
	@# No message state may claim transmission. This estate has no transport and
	@# the table is where that stops being a matter of memory.
	@test "$$(sed -n '/bf_message_states\[\] = {/,/^};/p' generated/native_missing_cells.h | grep -c ', 1, ')" = 0
	@rm -f generated/native-cell-contracts.verify.json generated/native-path-changes.verify.json generated/native-message-states.verify.json generated/native_missing_cells.verify.h
	@cd $(ROOT)/schemas/control && cue export . -e nativeCriterionKinds --out json > ../../cmd/BonfyreWebAddress/generated/native-criterion-kinds.verify.json
	@cd $(ROOT)/schemas/control && cue export . -e nativeCandidateVerdicts --out json > ../../cmd/BonfyreWebAddress/generated/native-candidate-verdicts.verify.json
	@zig run tools/compile_discovery_sets.zig -- generated/native-criterion-kinds.verify.json generated/native-candidate-verdicts.verify.json generated/native_discovery_sets.verify.h
	@cmp -s generated/native_discovery_sets.h generated/native_discovery_sets.verify.h
	@# At least one verdict must be beyond a machine's reach, or nothing about a
	@# working set stops at a person.
	@test "$$(sed -n '/bf_candidate_verdicts\[\] = {/,/^};/p' generated/native_discovery_sets.h | grep -c ', 0, ')" -ge 1
	@rm -f generated/native-criterion-kinds.verify.json generated/native-candidate-verdicts.verify.json generated/native_discovery_sets.verify.h
	@cd $(ROOT)/schemas/control && cue export . -e nativeInteractionPrimitives --out json > ../../cmd/BonfyreWebAddress/generated/native-interaction-primitives.verify.json
	@cd $(ROOT)/schemas/control && cue export . -e nativeCloneGrammars --out json > ../../cmd/BonfyreWebAddress/generated/native-clone-grammars.verify.json
	@zig run tools/compile_clone_grammars.zig -- generated/native-interaction-primitives.verify.json generated/native-clone-grammars.verify.json generated/native_clone_grammars.verify.h
	@cmp -s generated/native_clone_grammars.h generated/native_clone_grammars.verify.h
	@# Thirty grammars, and the count is asserted so a thirty-first cannot be
	@# added without somebody deciding it belongs.
	@test "$$(sed -n '/bf_clone_grammars\[\] = {/,/^};/p' generated/native_clone_grammars.h | grep -c '^  {')" = 30
	@# A page may not compute density itself; the shared counts live in the table.
	@! grep -q 'shared_by =' src/main.c
	@rm -f generated/native-interaction-primitives.verify.json generated/native-clone-grammars.verify.json generated/native_clone_grammars.verify.h
	@cd $(ROOT)/schemas/control && cue export . -e nativeDestinationStates --out json > ../../cmd/BonfyreWebAddress/generated/native-destination-states.verify.json
	@cd $(ROOT)/schemas/control && cue export . -e nativeChangeConsequences --out json > ../../cmd/BonfyreWebAddress/generated/native-change-consequences.verify.json
	@cd $(ROOT)/schemas/control && cue export . -e nativeDestinationCarries --out json > ../../cmd/BonfyreWebAddress/generated/native-destination-carries.verify.json
	@zig run tools/compile_destination_release.zig -- generated/native-destination-states.verify.json generated/native-change-consequences.verify.json generated/native-destination-carries.verify.json generated/native_destination_release.verify.h
	@cmp -s generated/native_destination_release.h generated/native_destination_release.verify.h
	@# Exactly three states are deliveries. A fourth cannot join them quietly,
	@# because the whole point of the table is that readiness is not delivery.
	@test "$$(sed -n '/bf_destination_states\[\] = {/,/^};/p' generated/native_destination_release.h | grep -c ', 1, [01], ')" = 3
	@rm -f generated/native-destination-states.verify.json generated/native-change-consequences.verify.json generated/native-destination-carries.verify.json generated/native_destination_release.verify.h
	@# The operator grammar and the record affinities were hand-generated and
	@# unguarded, which is the same silence the rest of these rules exist to
	@# break: a header that has drifted from its CUE gates nothing.
	@cd $(ROOT)/schemas/control && cue export . -e nativeOperatorArchetypes --out json > ../../cmd/BonfyreWebAddress/generated/native-operator-archetypes.verify.json
	@cd $(ROOT)/schemas/control && cue export . -e nativeProjectFacets --out json > ../../cmd/BonfyreWebAddress/generated/native-project-facets.verify.json
	@cd $(ROOT)/schemas/control && cue export . -e nativeRecordAffinities --out json > ../../cmd/BonfyreWebAddress/generated/native-record-affinities.verify.json
	@cd $(ROOT)/schemas/control && cue export . -e nativeHouseholdReach --out json > ../../cmd/BonfyreWebAddress/generated/native-household-reach.verify.json
	@zig run tools/compile_operators.zig -- generated/native-operator-archetypes.verify.json generated/native-project-facets.verify.json generated/native-record-affinities.verify.json generated/native-household-reach.verify.json generated/native_operators.verify.h
	@cmp -s generated/native_operators.h generated/native_operators.verify.h
	@grep -q 'BF_RECORD_AFFINITY_COUNT' generated/native_operators.h
	@grep -q 'BF_HOUSEHOLD_REACH_COUNT' generated/native_operators.h
	@# Household reach is a declared name matched as a whole segment. A substring
	@# test is what made no_of_employees into somebody's household.
	@! grep -q 'strstr(fname' src/main.c
	@# An affinity may only require a property the compiler actually measures
	@# off a DocType's fields. A sixth would be a resemblance nobody evidenced.
	@test "$$(grep -o '"\(money\|composition\|place\|submittable\|household\)"' generated/native_operators.h | sort -u | wc -l | tr -d ' ')" = 5
	@rm -f generated/native-operator-archetypes.verify.json generated/native-project-facets.verify.json generated/native-record-affinities.verify.json generated/native-household-reach.verify.json generated/native_operators.verify.h
	@cd $(ROOT)/schemas/control && cue export . -e nativeInstitutionPlanes --out json > ../../cmd/BonfyreWebAddress/generated/native-planes.verify.json
	@zig run tools/compile_planes.zig -- generated/native-planes.verify.json generated/native_planes.verify.h
	@cmp -s generated/native_planes.h generated/native_planes.verify.h
	@grep -q 'BF_INSTITUTION_PLANE_COUNT' generated/native_planes.h
	@# A granted plane must never be generated as public: the room's whole
	@# distinction rests on the contract rather than on what it remembers.
	@test "$$(grep -c '"granted"' generated/native_planes.h)" = 4
	@rm -f generated/native-planes.verify.json generated/native_planes.verify.h
	@# The governance grammar is generated for the same reason the worlds table
	@# is: a declaration nothing reads gates nothing, and a refusal that lives
	@# only in a comment is one the page never prints.
	@cd $(ROOT)/schemas/control && cue export . -e nativeGovernanceGrammar --out json > ../../cmd/BonfyreWebAddress/generated/native-governance-verbs.verify.json
	@cd $(ROOT)/schemas/control && cue export . -e nativeGovernanceBackings --out json > ../../cmd/BonfyreWebAddress/generated/native-governance-backings.verify.json
	@zig run tools/compile_governance.zig -- generated/native-governance-verbs.verify.json generated/native-governance-backings.verify.json generated/native_governance.verify.h
	@cmp -s generated/native_governance.h generated/native_governance.verify.h
	@grep -q 'BF_GOVERNANCE_VERB_COUNT' generated/native_governance.h
	@# Only a backing that can settle may claim to; the count is asserted so a
	@# fifth settling backing cannot appear without someone noticing.
	@test "$$(grep -c ', 1, ' generated/native_governance.h)" = 3
	@rm -f generated/native-governance-verbs.verify.json generated/native-governance-backings.verify.json generated/native_governance.verify.h
	@cd $(ROOT)/schemas/control && cue export . -e nativePageComposition --out json > ../../cmd/BonfyreWebAddress/generated/native-page-composition.verify.json
	@zig run tools/compile_page_composition.zig -- generated/native-page-composition.verify.json generated/native_page_composition.verify.h
	@cmp -s generated/native_page_composition.h generated/native_page_composition.verify.h
	@rm -f generated/native-page-composition.verify.json generated/native_page_composition.verify.h
	@# The page executor may branch on topology and material only: a region role,
	@# grammar or World name inside it would be a route-local page in disguise.
	@! sed -n '/static void page_emit_region(/,/static int page_emit_regions(/p' src/main.c | grep -q 'region->role\|strcmp(world\|"stage"\|"watch"\|"listen"'
	@! sed -n '/static int emit_native_world_page(/,/static int emit_native_world_shelf(/p' src/main.c | grep -q 'occurrenceCard\|Reading boundary\|this World'
	@cd $(ROOT)/schemas/control && cue export . -e nativeAudiencePresentations --out json > ../../cmd/BonfyreWebAddress/generated/native-audience-presentations.verify.json
	@zig run tools/compile_audience_presentations.zig -- generated/native-audience-presentations.verify.json generated/native_audience_presentations.verify.h
	@cmp -s generated/native_audience_presentations.h generated/native_audience_presentations.verify.h
	@grep -q 'BF_NATIVE_AUDIENCE_PRESENTATION_COUNT' generated/native_audience_presentations.h
	@! grep -q 'static const BfAudienceCopy AUDIENCE_COPY' src/main.c
	@rm -f generated/native-audience-presentations.verify.json generated/native_audience_presentations.verify.h

$(DOMAIN_PROFILE_TEST): tests/domain_profile_projection_test.c src/domain_profile_projection.c src/domain_profile_projection.h $(DOMAIN_PROFILE_HEADER) $(ROOT)/engine/core/src/information_policy.c
	$(CC) $(CFLAGS) -Isrc -I$(ROOT)/engine/core/include -o $@ tests/domain_profile_projection_test.c src/domain_profile_projection.c $(ROOT)/engine/core/src/information_policy.c

.PHONY: domain-profile-witness domain-profile-compiler-witness
all: domain-profile-compiler-witness domain-profile-witness
test: domain-profile-compiler-witness
domain-profile-compiler-witness:
	node --test tests/domain_profile_compiler.test.mjs

domain-profile-witness: $(DOMAIN_PROFILE_TEST)
	./$(DOMAIN_PROFILE_TEST)

.PHONY: github-app-route-witness
all test: github-app-route-witness
bonfyre-web-address: | github-app-route-witness
github-app-route-witness: tests/connect_github_app_route_test.c src/connect_github_app.h
	@set -eu; witness=$$(mktemp "$${TMPDIR:-/tmp}/bonfyre-github-route.XXXXXX"); \
	trap 'rm -f "$$witness"' 0 1 2 15; \
	$(CC) $(CFLAGS) tests/connect_github_app_route_test.c -o "$$witness"; "$$witness"
# CMS route composition is inherited by nested pages and controlled sign-in hosts.
ROUTE_PRESENTATION_TEST := build/route-presentation-test
$(ROUTE_PRESENTATION_TEST): tests/route_presentation_test.c src/route_presentation.h src/site_presentation.c src/site_presentation.h | build
	$(CC) $(CFLAGS) $(BUILD_INCLUDES) -o $@ tests/route_presentation_test.c src/site_presentation.c -lsqlite3
.PHONY: route-presentation-witness
all test: route-presentation-witness
route-presentation-witness: $(ROUTE_PRESENTATION_TEST)
	./$(ROUTE_PRESENTATION_TEST)


$(PEOPLE_PROJECTION_TEST): tests/people_projection_test.c src/people_projection.c src/people_projection.h src/people_name_filter.h $(ROOT)/lib/libbonfyre/include/bf_sqlite_path.h src/credit_profile_host.h $(ROOT)/lib/libbonfyre/src/bf_sha256.c
	$(CC) $(CFLAGS) $(BUILD_INCLUDES) -o $@ tests/people_projection_test.c $(ROOT)/lib/libbonfyre/src/bf_sha256.c -lsqlite3

people-projection-witness: $(PEOPLE_PROJECTION_TEST)
	./$(PEOPLE_PROJECTION_TEST)

people-static-profile-witness: $(PEOPLE_STATIC_MANIFEST)
	sh ./tests/people_static_manifest_test.sh

people-static-manifest-witness:
	sh ./tests/people_static_manifest_test.sh
	sh ../../scripts/tests/people_ledger_static_export_test.sh

$(PEOPLE_STATIC_MANIFEST): tools/people_static_manifest.c src/people_name_filter.h src/credit_profile_host.h $(ROOT)/lib/libbonfyre/src/bf_sha256.c $(ROOT)/lib/libbonfyre/include/bonfyre.h $(ROOT)/lib/libbonfyre/include/bf_static_query.h
	$(CC) $(CFLAGS) -I$(ROOT)/lib/libbonfyre/include -o $@ tools/people_static_manifest.c $(ROOT)/lib/libbonfyre/src/bf_sha256.c -lsqlite3

people-static-manifest-witness: $(PEOPLE_STATIC_MANIFEST)

$(BOUNDED_OUTPUT_TEST): tests/bounded_output_test.c src/bounded_output.c src/bounded_output.h
	$(CC) $(CFLAGS) -Isrc -o $@ tests/bounded_output_test.c src/bounded_output.c

test: $(TARGET) $(PROJECTION_TEST) $(TERRITORY_HOME_TEST) $(DOMAIN_PROFILE_TEST) $(PEOPLE_PROJECTION_TEST) $(ATOM_FEED_TEST) $(SUBJECT_ADDRESS_TEST) $(BOUNDED_OUTPUT_TEST)
	@if command -v ast-grep >/dev/null 2>&1; then $(MAKE) static-audit-check; else echo 'ast-grep unavailable: structural audit skipped'; fi
	./$(BOUNDED_OUTPUT_TEST)
	./$(ATOM_FEED_TEST)
	./$(SUBJECT_ADDRESS_TEST)
	$(MAKE) surface-programs-verify
	./$(PROJECTION_TEST)
	./$(TERRITORY_HOME_TEST)
	./$(DOMAIN_PROFILE_TEST)
	./$(PEOPLE_PROJECTION_TEST)
	$(MAKE) people-static-manifest-witness
	$(MAKE) -C ../BonfyreAuth all
	$(MAKE) -C ../BonfyreUpper90 all
	./tests/web_address_test.sh
	./tests/a2a_surface_test.sh
	./tests/auth_surface_test.sh ./$(TARGET)
	./tests/external_account_surface_test.sh ./$(TARGET)
	./tests/private_estate_surface_test.sh ./$(TARGET)
	./tests/site_program_native_test.sh ./$(TARGET)
	$(MAKE) foundry-behavior-witness
	./tests/page_composition_test.sh ./$(TARGET)
	./tests/page_census_reader_test.sh ./$(TARGET)
	./tests/page_programs_test.sh ./$(TARGET)
	sh ./tests/compiled_subject_route_web_test.sh ./$(TARGET)
	./tests/page_items_test.sh ./$(TARGET)
	./tests/page_saved_test.sh ./$(TARGET)
	./tests/page_parcel_test.sh ./$(TARGET)
	./tests/page_commons_test.sh ./$(TARGET)
	./tests/page_public_commons_test.sh ./$(TARGET)
	./tests/page_events_test.sh ./$(TARGET)
	./tests/page_transport_test.sh ./$(TARGET)
	./tests/page_stage_test.sh ./$(TARGET)
	./tests/page_surface_scene_test.sh ./$(TARGET)
	./tests/web_address_concurrency_test.sh ./$(TARGET)
	./tests/native_surface_routes_test.sh ./$(TARGET)
	./tests/nginx_edge_test.sh
	./tests/cloudflared_ingress_test.sh
	./tests/compose_service_test.sh
all: public-home-search-witness
$(PUBLIC_HOME_SEARCH_TEST): tests/public_home_search_test.c $(PUBLIC_HOME_SEARCH_SOURCE) src/public_home_search.h
	$(CC) $(CFLAGS) -Isrc -o $@ tests/public_home_search_test.c $(PUBLIC_HOME_SEARCH_SOURCE)
public-home-search-witness: $(PUBLIC_HOME_SEARCH_TEST)
	./$(PUBLIC_HOME_SEARCH_TEST)
MANAGED_PREFIX ?= $(HOME)/.bonfyre
install: $(TARGET)
	install -d "$(MANAGED_PREFIX)/bin"
	install -m 755 $(TARGET) "$(MANAGED_PREFIX)/bin/.bonfyre-web-address.next"
	mv -f "$(MANAGED_PREFIX)/bin/.bonfyre-web-address.next" "$(MANAGED_PREFIX)/bin/bonfyre-web-address"

clean:
	rm -f $(TARGET) $(SITE_TARGET) $(PEOPLE_STATIC_MANIFEST) $(PROJECTION_TEST) $(TERRITORY_HOME_TEST) $(PUBLIC_HOME_SEARCH_TEST) $(DOMAIN_PROFILE_TEST) $(PEOPLE_PROJECTION_TEST) $(ATOM_FEED_TEST) $(SUBJECT_ADDRESS_TEST) $(BOUNDED_OUTPUT_TEST) $(OBJECTS) $(OBJECTS:.o=.d)

.PHONY: spatial-frontier-witness
spatial-frontier-witness: $(TARGET)
	$(MAKE) -C ../BonfyreSpatial bonfyre-spatial
	$(MAKE) -C ../BonfyreExternalEstate bonfyre-external-estate
	./tests/spatial_frontier_route_test.sh ./$(TARGET) ../BonfyreSpatial/bonfyre-spatial ../BonfyreExternalEstate/bonfyre-external-estate

.PHONY: recipe-week-witness
recipe-week-witness: $(TARGET)
	$(MAKE) -C ../BonfyreRecipe build/bonfyre-recipe
	bun tests/week_web.test.mjs ./$(TARGET)
test: recipe-week-witness

.PHONY: url-review-witness
url-review-witness: $(TARGET)
	bun test tests/url_review.test.mjs
test: url-review-witness

.PHONY: http-readiness-witness
http-readiness-witness: $(TARGET)
	bun test tests/http_readiness.test.mjs
test: http-readiness-witness

.PHONY: auth-continuation-witness auth-continuation-browser
auth-continuation-witness: $(TARGET)
	test -x ../BonfyreAuth/bonfyre-auth
	test -x ../BonfyreCMS/bonfyre-cms
	bun tests/auth_continuation_test.mjs
test: auth-continuation-witness

# Chrome and local TLS are opt-in; the browser test checks available memory first.
auth-continuation-browser: $(TARGET)
	bun tests/auth_continuation_browser.mjs

.PHONY: pack-setup-witness
pack-setup-witness: $(TARGET)
	$(MAKE) -C ../BonfyreCMS bonfyre-cms
	bun tests/pack_setup_test.mjs
test: pack-setup-witness

.PHONY: addressed-depth-witness
addressed-depth-witness: $(TARGET)
	sh tests/addressed_depth_test.sh ./$(TARGET)
test: addressed-depth-witness

ESTATE_REFERENCE_TEST := tests/estate-reference-test
$(ESTATE_REFERENCE_TEST): tests/estate_reference_test.c src/estate_reference.h $(ROOT)/lib/libbonfyre/include/bf_sqlite_path.h
	$(CC) $(CFLAGS) -I$(ROOT)/lib/libbonfyre/include -o $@ tests/estate_reference_test.c -lsqlite3
.PHONY: estate-reference-witness
estate-reference-witness: $(ESTATE_REFERENCE_TEST)
	./$(ESTATE_REFERENCE_TEST)
test: estate-reference-witness

# Uses real isolated owners and a pinned ephemeral TLS certificate in Chrome.
# Keep browser work opt-in like the existing continuation browser target.
.PHONY: account-navigation-browser
account-navigation-browser: $(TARGET)
	bun test tests/account_navigation_refresh.test.mjs

auth-continuation-browser: account-navigation-browser

.PHONY: page-presentation-witness
all: page-presentation-witness
test: page-presentation-witness
page-presentation-witness: build/page-style-token-test build/page-preview-region-test
	./build/page-style-token-test
	./build/page-preview-region-test
build/page-style-token-test: tests/page_style_token_test.c src/page_style_token.h
	@mkdir -p build
	$(CC) $(CFLAGS) -o $@ tests/page_style_token_test.c -lsqlite3
build/page-preview-region-test: tests/page_preview_region_test.c src/page_preview_region.h $(PAGE_COMPOSITION_HEADER)
	@mkdir -p build
	$(CC) $(CFLAGS) -o $@ tests/page_preview_region_test.c

# Public source material must survive editorial grouping and mixed artwork.
# These are isolated native-owner reads, not external fetching or playback.
.PHONY: public-rss-presentation-witness
public-rss-presentation-witness: $(TARGET)
	bun test tests/public_rss_presentation_test.mjs tests/house_presentation_test.mjs
test: public-rss-presentation-witness

.PHONY: production-home-context-witness
production-home-context-witness: $(TARGET)
	bun test tests/production_home_context.test.mjs tests/production_public_projection.test.mjs
test: production-home-context-witness
.PHONY: source-publication-witness authored-publication-browser
source-publication-witness: $(TARGET)
	$(MAKE) -C ../BonfyreCMS bonfyre-cms
	bun test tests/source_publication_web.test.mjs
test: source-publication-witness
authored-publication-browser: $(TARGET)

.PHONY: account-source-cohesion-witness
test: account-source-cohesion-witness
account-source-cohesion-witness: $(TARGET)
	bun test tests/account_source_integrated.test.mjs ../../site/matrix/surfaces/build/source_cut.test.mjs ../../site/matrix/surfaces/build/authored_rows.test.mjs
	sh ../../site/matrix/surfaces/build/link_workspace_test.sh

# Shared visible/device seeking can be checked without a browser or live media.
.PHONY: public-audio-window-witness public-media-browser
all: public-audio-window-witness
test: public-audio-window-witness
public-audio-window-witness:
	bun test tests/public_audio_window.test.mjs

# Opt-in native RSS, real generated audio and independently decoded public QR.
public-media-browser: $(TARGET)
	bun test tests/public_code_test.mjs tests/public_audio_deck_test.mjs

# Retained source work uses the native gateway/codec and existing record carrier.
.PHONY: source-web-work-witness source-web-work-browser
test: source-web-work-witness
source-web-work-witness:
	bun test tests/source_web_work.test.mjs ../BonfyreSourceFrontier/tests/web_work.test.mjs ../../integrations/browser/tests/workspace-value.test.mjs ../../integrations/browser/tests/workspace-save-snapshot.test.mjs ../../integrations/browser/tests/workspace-metadata.test.mjs

source-web-work-browser:
	bun test --timeout 30000 tests/web_workbench.browser.test.mjs

.PHONY: account-world-workspace-test
account-world-workspace-test: $(TARGET)
	sh tests/account_world_workspace_test.sh
.PHONY: account-world-audio-test
account-world-audio-test: $(TARGET)
	sh tests/account_world_audio_test.sh
.PHONY: account-world-cinema-test
account-world-cinema-test: $(TARGET)
	sh tests/account_world_cinema_test.sh
build/web_main.o build/compact/web_main.o: src/account_world_cinema_web.h

# Fast local preview of the tree build against copies of the live stores.
# Never touches the live service; see tools/preview.sh for the browser recipe.
.PHONY: preview preview-refresh preview-stop
preview: $(TARGET)
	@sh tools/preview.sh serve
preview-refresh: $(TARGET)
	@sh tools/preview.sh refresh
preview-stop:
	@sh tools/preview.sh stop
