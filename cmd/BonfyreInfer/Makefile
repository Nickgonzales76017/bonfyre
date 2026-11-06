# BonfyreInfer retained-body build adapter
#
# The current source frontier retains the compiled planning implementation
# (src/main.o and src/graph_plan.o) but not its C translation units.  Relinking
# those bodies is the lawful current build; model architecture ownership stays
# with libcolibri-bonfyre and the distributed execution path stays with
# BonfyreMoE.  `clean` deliberately preserves the unique retained objects.

CC      ?= cc
CFLAGS  ?= -O3 -Wall -Wextra
TARGET   = bonfyre-infer
OBJECTS  = src/main.o src/graph_plan.o

.PHONY: all clean test

all: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CC) $(CFLAGS) $(OBJECTS) -o $@

test: $(TARGET)
	@./$(TARGET) plan --arch qwen2 | grep -q '"n_routed_experts": 0'
	@./$(TARGET) plan --arch glm52 | grep -q '"n_routed_experts": 12288'
	@echo "BonfyreInfer retained-body conformance: PASS"

clean:
	@rm -f $(TARGET)
	@echo "Preserved retained bodies: $(OBJECTS)"
