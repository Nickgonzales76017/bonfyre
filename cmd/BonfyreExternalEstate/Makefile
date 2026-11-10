CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11 -ffunction-sections -fdata-sections
CFLAGS += -I../BonfyreLex/src
CURL_CFLAGS ?= $(shell pkg-config --cflags libcurl 2>/dev/null)
CURL_LIBS ?= $(shell pkg-config --libs libcurl 2>/dev/null)
LOCAL_CFLAGS = -I../../lib/libbonfyre/include -I../../engine/core/include
LOCAL_LIBS = -L../../lib/libbonfyre -lbonfyre ../../engine/core/libbonfyre-fabric.a

TARGET := bonfyre-external-estate

.PHONY: all clean test commerce-observation-witness incumbent-coordination-witness scene-source-witness
all: $(TARGET)

$(TARGET): src/main.c src/address_resolve.c src/address_resolve.h ../../engine/core/libbonfyre-fabric.a ../../lib/libbonfyre/include/bf_command_lookup.h
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) $(CURL_CFLAGS) -Wl,-dead_strip -o $@ src/main.c src/address_resolve.c $(LOCAL_LIBS) -lsqlite3 $(if $(CURL_LIBS),$(CURL_LIBS),-lcurl)

../../engine/core/libbonfyre-fabric.a:
	$(MAKE) -C ../../engine/core

test: $(TARGET) ../BonfyreModel/bonfyre-model commerce-observation-witness incumbent-coordination-witness scene-source-witness program-resolution-witness
	$(MAKE) -C ../BonfyreCompress all
	./tests/external_estate_test.sh ./$(TARGET)
	./tests/model_representation_owner_test.sh ./$(TARGET) ../BonfyreModel/bonfyre-model
	bun test ./tests/compile_live_sources_test.mjs

.PHONY: pack-lock-advertisement-witness
pack-lock-advertisement-witness: $(TARGET)
	$(MAKE) -C ../BonfyreCMS bonfyre-cms
	$(MAKE) -C ../BonfyreCMS -f tests/program_continuation_test.mk test
test: pack-lock-advertisement-witness

../BonfyreModel/bonfyre-model:
	$(MAKE) -C ../BonfyreModel all

all: native-witness participation-witness

# The evidence contract and, more to the point, what it refuses. A reader that
# counts any document carrying the right field names lets every owner launder
# itself onto any rung.
participation-witness: $(TARGET)
	./tests/participation_test.sh ./$(TARGET)
	./tests/route_cost_test.sh ./$(TARGET)

native-witness: $(TARGET)
	./tests/external_estate_test.sh ./$(TARGET)
	./tests/subject_fabric_precompute_test.sh
	./tests/subject_projection_precompute_test.sh
	bun test ./tests/foundation_subject_projection.test.mjs

# Pure compiler witness: it neither fetches providers nor initiates commerce.
commerce-observation-witness:
	bun test ./tests/commerce_family.test.mjs

incumbent-coordination-witness:
	bun test ./tests/coordination_family.test.mjs
	bun test ./tests/incumbent_materialization.test.mjs
	bun test ./tests/public_carrier_collector.test.mjs
	sh ./tests/incumbent_ingest_owner_test.sh
	node ../../integrations/wordpress/bridge.test.mjs
	node ../../integrations/wordpress/plugin.test.mjs

scene-source-witness:
	bun test ./tests/recursive_channels.test.mjs
	bun test ./tests/source_frontier_accounting.test.mjs
	bun test ./tests/source_passports.test.mjs
	bun test ./tests/dataset_registry.test.mjs
	bun test ./tests/decision_program_plane.test.mjs
	bun test ./tests/source_pressure.test.mjs
	bun test ./tests/source_change_impact.test.mjs
	bun test ./tests/source_fabric_cycle_impact.test.mjs
	bun test ./tests/source_forward.test.mjs
	bun test ./tests/source_attention.test.mjs
	bun test ./tests/source_attention_sync.test.mjs
	bun test ./tests/scene_source_receipts.test.mjs
	bun test ./tests/scene_source_delta.test.mjs
	bun test ./tests/scene_legacy_sources.test.mjs
	bun test ./tests/source_starvation.test.mjs

clean:
	rm -f $(TARGET)

.PHONY: program-resolution-witness
program-resolution-witness: $(TARGET)
	$(MAKE) -C ../BonfyreCMS bonfyre-cms
	$(MAKE) -C ../BonfyreLoweringWorkers bonfyre-lowering-workers
	bun test ../../scripts/tests/program_resolution_native.test.mjs ../../scripts/tests/compile_program_fabric.test.mjs

# Typed source-to-experience routing has its own fast, fixture-only witness.
.PHONY: source-experience-witness
all: source-experience-witness
source-experience-witness:
	bun test ./tests/source_experience_routing.test.mjs
