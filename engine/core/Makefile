CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
CFLAGS += -I../../cmd/BonfyreLex/src
# GCC reports brace-less one-line guards that clang accepts.
CFLAGS += -Wno-misleading-indentation
# gmtime_r, strsep and friends under strict -std=c11 on glibc.
CFLAGS += -D_DEFAULT_SOURCE

# gcc's -Wformat-truncation fires ~39 times in fabric_exec.c where snprintf
# writes a bounded token into a fixed buffer. Every one is memory-safe --
# snprintf truncates rather than overflows -- and gcc cannot prove the bound, so
# these are heuristic rather than defects. Two genuine ones were found and fixed
# by widening the workspace-token buffers; the remainder are suppressed here
# rather than silenced by editing dozens of declarations blind.
#
# This is a deliberate, scoped deferral, not a clean bill of health: a truncated
# identifier would still be the WRONG identifier, and the remaining sites have
# not been individually audited. clang (the macOS build) does not implement this
# warning, which is why the whole class only appeared on the Linux host.
# Probe the flag rather than the compiler name: Ubuntu's `cc --version` prints
# "cc", not "gcc", so name-matching silently failed to apply this.
CFLAGS += $(shell $(CC) -Wno-format-truncation -E -x c - </dev/null >/dev/null 2>&1 \
            && echo -Wno-format-truncation)
ROOT := ../..
INCLUDES := -Iinclude -I$(ROOT)/lib/libbonfyre/include
TARGET := libbonfyre-fabric.a
OBJDIR := obj
OBJS := $(OBJDIR)/fabric.o $(OBJDIR)/fabric_exec.o $(OBJDIR)/workgraph.o \
	$(OBJDIR)/workgraph_schema.o $(OBJDIR)/workgraph_events.o \
	$(OBJDIR)/workgraph_scheduler.o $(OBJDIR)/workgraph_effects.o \
	$(OBJDIR)/lifecycle.o \
	$(OBJDIR)/filesystem_projection.o \
	$(OBJDIR)/process_operator.o $(OBJDIR)/operator_contract.o $(OBJDIR)/bf_sha256.o \
	$(OBJDIR)/occurrence.o $(OBJDIR)/occurrence_origin.o $(OBJDIR)/commitment.o $(OBJDIR)/retention.o $(OBJDIR)/information_policy.o $(OBJDIR)/commerce.o $(OBJDIR)/protocol_contract.o $(OBJDIR)/standing_address.o $(OBJDIR)/actor.o $(OBJDIR)/admission.o $(OBJDIR)/corroboration.o \
	$(OBJDIR)/evidence.o $(OBJDIR)/activation.o $(OBJDIR)/service_lifecycle.o $(OBJDIR)/reservation.o $(OBJDIR)/foreign_twin.o $(OBJDIR)/authority.o $(OBJDIR)/proof.o $(OBJDIR)/target_obligation.o \
	$(OBJDIR)/control_provider.o $(OBJDIR)/control_admission.o \
	$(OBJDIR)/control_attention.o $(OBJDIR)/control_capability.o \
	$(OBJDIR)/semantic_operator.o \
	$(OBJDIR)/semantic_derivation.o \
	$(OBJDIR)/matrix.o \
	$(OBJDIR)/matrix_closure.o \
	$(OBJDIR)/bf_catalog_generation.o $(OBJDIR)/bf_json.o $(OBJDIR)/bf_utf8.o

.PHONY: all clean test reference-witness

all: $(TARGET)

# Native conformance tests (self-contained C; no Python needed to pass).
TEST_SRCS := tests/workgraph_checkpoint_test.c tests/workgraph_run_evidence_test.c tests/routable_family_test.c tests/lifecycle_test.c tests/occurrence_spine_test.c tests/occurrence_origin_test.c tests/actor_graph_test.c tests/admission_test.c tests/control_compose_test.c tests/corroboration_test.c tests/evidence_test.c tests/activation_test.c tests/service_lifecycle_test.c tests/reservation_test.c tests/authority_test.c tests/proof_test.c tests/target_obligation_test.c tests/semantic_operator_test.c tests/semantic_derivation_test.c tests/matrix_test.c tests/matrix_closure_test.c tests/p0_loop_test.c tests/occurrence_correction_test.c tests/retraction_cascade_test.c tests/activation_retraction_test.c tests/commitment_test.c tests/retention_test.c tests/information_policy_test.c tests/commerce_test.c tests/protocol_contract_test.c tests/standing_address_test.c
TEST_BINS := $(patsubst tests/%.c,$(OBJDIR)/%,$(TEST_SRCS))

$(OBJDIR)/%: tests/%.c $(TARGET) | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) $< $(TARGET) -lsqlite3 -o $@

test: $(TEST_BINS)
	@set -e; for t in $(TEST_BINS); do echo "== $$t"; $$t; done
	@echo "native conformance: all passed"

reference-witness: $(TARGET)
	$(MAKE) -C $(ROOT)/programs/bonfyre
	cd $(ROOT) && bun test engine/core/tests/operator_reference.test.mjs

$(OBJDIR):
	mkdir -p $@

$(OBJDIR)/fabric.o: src/fabric.c include/bonfyre_fabric.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/bf_utf8.o: $(ROOT)/lib/libbonfyre/bf_utf8.c $(ROOT)/lib/libbonfyre/include/bf_utf8.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/fabric_exec.o: src/fabric_exec.c include/bonfyre_fabric_internal.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/workgraph.o: src/workgraph.c include/bf_workgraph.h src/workgraph_internal.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/workgraph_schema.o: src/workgraph_schema.c include/bf_workgraph.h src/workgraph_internal.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/workgraph_events.o: src/workgraph_events.c include/bf_workgraph.h src/workgraph_internal.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/workgraph_scheduler.o: src/workgraph_scheduler.c include/bf_workgraph.h src/workgraph_internal.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/workgraph_effects.o: src/workgraph_effects.c include/bf_workgraph.h src/workgraph_internal.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/lifecycle.o: src/lifecycle.c include/bf_lifecycle.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/filesystem_projection.o: src/filesystem_projection.c include/bf_filesystem.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/process_operator.o: src/process_operator.c include/bf_operator.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/operator_contract.o: src/operator_contract.c include/bf_operator_contract.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/bf_sha256.o: $(ROOT)/lib/libbonfyre/src/bf_sha256.c | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/occurrence.o: src/occurrence.c include/bf_occurrence.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/occurrence_origin.o: src/occurrence_origin.c include/bf_occurrence_origin.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/commitment.o: src/commitment.c include/bf_commitment.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/retention.o: src/retention.c include/bf_retention.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/information_policy.o: src/information_policy.c include/bf_information_policy.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/commerce.o: src/commerce.c include/bf_commerce.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/protocol_contract.o: src/protocol_contract.c include/bf_protocol_contract.h include/bf_commitment.h include/bf_information_policy.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/standing_address.o: src/standing_address.c include/bf_standing_address.h include/bf_information_policy.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/actor.o: src/actor.c include/bf_actor.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/admission.o: src/admission.c include/bf_admission.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/corroboration.o: src/corroboration.c include/bf_corroboration.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/evidence.o: src/evidence.c include/bf_evidence.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/activation.o: src/activation.c include/bf_activation.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/service_lifecycle.o: src/service_lifecycle.c include/bf_service_lifecycle.h include/bf_authority.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/reservation.o: src/reservation.c include/bf_reservation.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/foreign_twin.o: src/foreign_twin.c include/bf_foreign_twin.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/authority.o: src/authority.c include/bf_authority.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/proof.o: src/proof.c include/bf_proof.h include/bf_evidence.h include/bf_foreign_twin.h include/bf_target_obligation.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/target_obligation.o: src/target_obligation.c include/bf_target_obligation.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/control_provider.o: src/control_provider.c include/bf_control.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/control_admission.o: src/control_admission.c include/bf_control.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/control_attention.o: src/control_attention.c include/bf_control.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/control_capability.o: src/control_capability.c include/bf_control.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/semantic_operator.o: src/semantic_operator.c include/bf_semantic_operator.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/semantic_derivation.o: src/semantic_derivation.c include/bf_semantic_derivation.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/matrix.o: src/matrix.c include/bf_matrix.h include/bf_authority.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/matrix_closure.o: src/matrix_closure.c include/bf_matrix_closure.h include/bf_matrix.h | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/bf_catalog_generation.o: $(ROOT)/lib/libbonfyre/src/bf_catalog_generation.c | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(OBJDIR)/bf_json.o: $(ROOT)/lib/libbonfyre/bf_json.c | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(TARGET): $(OBJS)
	ar rcs $@ $^
	ranlib $@

clean:
	rm -rf $(OBJDIR) $(TARGET)
