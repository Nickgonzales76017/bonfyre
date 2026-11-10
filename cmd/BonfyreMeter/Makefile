CC ?= cc
# -Oz: the installed estate is held to a byte budget (architecture/native-ratchet.cue);
# this organ is SQLite-bound, so size costs nothing measurable.
# An include path is not a tuning preference. `make all` passes CFLAGS through
# the environment, which makes `?=` skip this whole line -- so every -I on it
# vanished and the organ could not be rebuilt from scratch by the top-level
# build, only by its own. Optimisation stays overridable; what the source needs
# to compile at all is appended.
CFLAGS ?= -Oz -Wall -Wextra -std=c11
CFLAGS += -I../../lib/libbonfyre/include
TARGET = bonfyre-meter
SRC = src/main.c
# Unreferenced library code stays out of the installed program.
ifeq ($(shell uname -s),Darwin)
DEAD_STRIP = -Wl,-dead_strip
else
DEAD_STRIP = -Wl,--gc-sections
endif

all: $(TARGET) test test-work-costs test-token-evidence test-context-compare

$(TARGET): $(SRC) src/meter_packet.h src/meter_tokens.h src/meter_context.h
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC) ../../lib/libbonfyre/libbonfyre.a -lsqlite3 $(DEAD_STRIP)

clean:
	rm -f $(TARGET)

test: $(TARGET)
	sh tests/physical_attempt_test.sh ./$(TARGET)

test-work-costs: $(TARGET)
	sh tests/work_costs_test.sh ./$(TARGET)

test-token-evidence: $(TARGET)
	sh tests/token_evidence_test.sh ./$(TARGET)
	sh tests/token_archive_witness.sh ./$(TARGET)

test-context-compare: $(TARGET)
	$(MAKE) -C ../BonfyreQueue bonfyre-queue
	sh tests/context_compare_test.sh ./$(TARGET) ../BonfyreQueue/bonfyre-queue

.PHONY: all clean test test-work-costs test-token-evidence test-context-compare
