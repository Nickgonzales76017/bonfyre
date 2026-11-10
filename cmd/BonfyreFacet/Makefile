CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
CFLAGS += -I../BonfyreLex/src

TARGET := bonfyre-facet

.PHONY: all clean test native-witness
all: $(TARGET) native-witness agreement-witness density-witness

ROOT := ../..
XMAP := ../BonfyreExperienceMap
CORE_INCLUDES := -I$(ROOT)/engine/core/include -I$(ROOT)/lib/libbonfyre/include -I$(XMAP)/src
BRIDGE := $(XMAP)/src/protocol_derivation_bridge.c
# The participation digests are what make an owner's report checkable.
SHA := $(ROOT)/lib/libbonfyre/src/bf_sha256.c
CORE_LIB := $(ROOT)/engine/core/libbonfyre-fabric.a

$(TARGET): src/main.c $(BRIDGE) $(SHA) $(CORE_LIB) $(ROOT)/lib/libbonfyre/include/bf_command_lookup.h
	$(CC) $(CFLAGS) $(CORE_INCLUDES) -o $@ src/main.c $(BRIDGE) $(SHA) $(CORE_LIB) -lsqlite3

$(CORE_LIB):
	$(MAKE) -C $(ROOT)/engine/core

native-witness: $(TARGET)
	./tests/facet_test.sh ./$(TARGET)

# Two readers of one evidence file deriving one owner graph. They disagreed,
# and the estate's first hop was missing from every density report.
agreement-witness: $(TARGET)
	./tests/reader_agreement_test.sh ./$(TARGET)

test: native-witness

clean:
	rm -f $(TARGET)

# A grammar that is only names carries nothing. Fifty-nine application kinds sat
# outside the relation grammar because nobody wrote down what they are.
density-witness: $(TARGET)
	./tests/grammar_density_test.sh ./$(TARGET)
