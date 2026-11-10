CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
ROOT := ../..
GEN := generated
TARGET := bonfyre-experience-map
CORE := $(ROOT)/engine/core
CORE_LIB := $(CORE)/libbonfyre-fabric.a
CORE_INCLUDES := -I$(CORE)/include -I$(ROOT)/lib/libbonfyre/include -Isrc
PROTOCOL_BRIDGE_TEST := $(GEN)/protocol-derivation-bridge-test
PROTOCOL_OBSERVATION_BRIDGE_TEST := $(GEN)/protocol-observation-bridge-test

.PHONY: all corpus verify policy-check static-audit clean observed-bindings bind-ogc-observation bind-ogc-protocol core-lib protocol-derivation-bridge-witness protocol-derivation-bridge-verify protocol-observation-bridge-witness protocol-observation-bridge-verify native-corpus-smoke

all: corpus $(TARGET) native-corpus-smoke native-definition-witness

.PHONY: native-definition-witness
native-definition-witness: $(TARGET)
	$(MAKE) -C ../../tests/vectors bonfyre-vectors
	../../tests/vectors/bonfyre-vectors tests/vectors/native_definition.json tests/vectors/native_resolution.json

native-corpus-smoke: corpus $(TARGET)
	sh tests/native_corpus_resolution.test.sh

static-audit:
	@if command -v ast-grep >/dev/null 2>&1; then ast-grep scan -r analysis/no-runtime-compiler-spawn.yml tools --globs '*.mjs'; else echo 'static-audit: ast-grep unavailable'; fi

policy-check: static-audit
	$(MAKE) -C ../../tests/vectors bonfyre-vectors
	../../tests/vectors/bonfyre-vectors tests/vectors/policy_compilation.json
	bun test tests/atlas_bindings.test.mjs
	bun test tests/json_contract.test.mjs
	bun test tests/zero_close.test.mjs

corpus: policy-check
	bun tools/compile_experience_map.mjs --write
	bun test tests/compile_experience_map.test.mjs
	bun tools/materiality_report.mjs --write
	bun test tests/materiality_report.test.mjs
	$(MAKE) observed-bindings
	bun tools/participation_report.mjs --write
	bun test tests/participation_report.test.mjs
	bun test tests/domain_observation_bridge.test.mjs
	$(MAKE) protocol-derivation-bridge-witness
	$(MAKE) protocol-observation-bridge-witness
	bun tools/protocol_observation_bridge.mjs external:protocol-0304 --evidence generated/participation_evidence.jsonl --native-receipt generated/protocol_observation_bridge_receipt.json --output generated/participation_evidence.jsonl
	bun test tests/protocol_observation_bridge.test.mjs
	bun tools/participation_activation.mjs --write
	bun test tests/participation_activation.test.mjs
	bun tools/compile_stage_bindings.mjs --write
	bun test tests/compile_stage_bindings.test.mjs
	bun tools/audit_experience_map.mjs
	bun test tests/audit_experience_map.test.mjs

verify: policy-check
	bun tools/compile_experience_map.mjs --check
	bun test tests/compile_experience_map.test.mjs
	bun tools/materiality_report.mjs
	bun test tests/materiality_report.test.mjs
	bun tools/participation_report.mjs
	bun test tests/participation_report.test.mjs
	bun test tests/domain_observation_bridge.test.mjs
	$(MAKE) protocol-derivation-bridge-verify
	$(MAKE) protocol-observation-bridge-verify
	bun test tests/protocol_observation_bridge.test.mjs
	bun tools/participation_activation.mjs
	bun test tests/participation_activation.test.mjs
	bun test tests/compile_stage_bindings.test.mjs
	bun tools/audit_experience_map.mjs
	bun test tests/audit_experience_map.test.mjs

observed-bindings:
	@if [ -f generated/participation_evidence.jsonl ] && [ -f generated/ingest_protocol-0304/intake-manifest.json ]; then $(MAKE) bind-ogc-observation; $(MAKE) protocol-observation-bridge-witness; $(MAKE) bind-ogc-protocol; else echo "observed-bindings: no retained OGC evidence; skipping"; fi

bind-ogc-observation: policy-check
	bun tools/domain_observation_bridge.mjs external:protocol-0304 --observation-receipts generated/participation_evidence.jsonl --ingest-dir generated/ingest_protocol-0304 --output generated/participation_evidence.jsonl

$(PROTOCOL_OBSERVATION_BRIDGE_TEST): core-lib tests/protocol_observation_bridge_test.c src/protocol_derivation_bridge.c src/protocol_derivation_bridge.h
	mkdir -p $(GEN)
	$(CC) $(CFLAGS) $(CORE_INCLUDES) tests/protocol_observation_bridge_test.c src/protocol_derivation_bridge.c $(CORE_LIB) -lsqlite3 -o $@

protocol-observation-bridge-witness: $(PROTOCOL_OBSERVATION_BRIDGE_TEST)
	@./$(PROTOCOL_OBSERVATION_BRIDGE_TEST) > $(GEN)/protocol_observation_bridge_receipt.json

protocol-observation-bridge-verify: $(PROTOCOL_OBSERVATION_BRIDGE_TEST)
	@./$(PROTOCOL_OBSERVATION_BRIDGE_TEST) >/dev/null

bind-ogc-protocol:
	bun tools/protocol_observation_bridge.mjs external:protocol-0304 --evidence generated/participation_evidence.jsonl --native-receipt generated/protocol_observation_bridge_receipt.json --output generated/participation_evidence.jsonl

core-lib:
	$(MAKE) -C $(CORE)

$(PROTOCOL_BRIDGE_TEST): core-lib tests/protocol_derivation_bridge_test.c src/protocol_derivation_bridge.c src/protocol_derivation_bridge.h
	mkdir -p $(GEN)
	$(CC) $(CFLAGS) $(CORE_INCLUDES) tests/protocol_derivation_bridge_test.c src/protocol_derivation_bridge.c $(CORE_LIB) -lsqlite3 -o $@

protocol-derivation-bridge-witness: $(PROTOCOL_BRIDGE_TEST)
	@./$(PROTOCOL_BRIDGE_TEST) > $(GEN)/protocol_derivation_bridge_receipt.json

protocol-derivation-bridge-verify: $(PROTOCOL_BRIDGE_TEST)
	@./$(PROTOCOL_BRIDGE_TEST) >/dev/null

$(TARGET): src/main.c src/owner_resolution.c src/owner_resolution.h
	$(CC) $(CFLAGS) src/main.c src/owner_resolution.c -lsqlite3 -o $@
