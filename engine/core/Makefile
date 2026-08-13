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
	$(OBJDIR)/bf_catalog_generation.o

.PHONY: all clean

all: $(TARGET)

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

$(OBJDIR)/bf_catalog_generation.o: $(ROOT)/lib/libbonfyre/src/bf_catalog_generation.c | $(OBJDIR)
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(TARGET): $(OBJS)
	ar rcs $@ $^
	ranlib $@

clean:
	rm -rf $(OBJDIR) $(TARGET)
