CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
ROOT := ../..
INCLUDES := -Iinclude -I$(ROOT)/lib/libbonfyre/include
TARGET := libbonfyre-fabric.a
OBJDIR := obj
OBJS := $(OBJDIR)/fabric.o $(OBJDIR)/fabric_exec.o $(OBJDIR)/workgraph.o \
	$(OBJDIR)/workgraph_schema.o $(OBJDIR)/workgraph_events.o \
	$(OBJDIR)/workgraph_scheduler.o $(OBJDIR)/workgraph_effects.o \
	$(OBJDIR)/filesystem_projection.o \
	$(OBJDIR)/process_operator.o $(OBJDIR)/operator_contract.o $(OBJDIR)/bf_sha256.o \
	$(OBJDIR)/occurrence.o $(OBJDIR)/actor.o $(OBJDIR)/admission.o $(OBJDIR)/corroboration.o \
	$(OBJDIR)/evidence.o $(OBJDIR)/activation.o \
	$(OBJDIR)/bf_catalog_generation.o

.PHONY: all clean test

all: $(TARGET)

# Native conformance tests (self-contained C; no Python needed to pass).
TEST_SRCS := tests/routable_family_test.c tests/occurrence_spine_test.c tests/actor_graph_test.c tests/admission_test.c tests/control_compose_test.c tests/corroboration_test.c tests/evidence_test.c tests/activation_test.c
TEST_BINS := $(patsubst tests/%.c,$(OBJDIR)/%,$(TEST_SRCS))

$(OBJDIR)/%: tests/%.c $(TARGET) | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) $< $(TARGET) -lsqlite3 -o $@

test: $(TEST_BINS)
	@set -e; for t in $(TEST_BINS); do echo "== $$t"; $$t; done
	@echo "native conformance: all passed"

$(OBJDIR):
	mkdir -p $@

$(OBJDIR)/fabric.o: src/fabric.c include/bonfyre_fabric.h | $(OBJDIR)
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

$(OBJDIR)/bf_catalog_generation.o: $(ROOT)/lib/libbonfyre/src/bf_catalog_generation.c | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(TARGET): $(OBJS)
	ar rcs $@ $^
	ranlib $@

clean:
	rm -rf $(OBJDIR) $(TARGET)
