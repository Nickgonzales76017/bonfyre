# BonfyreFPQ — Functional Polar Quantization
# Store seed programs, not weights.
#
# Platform BLAS strategy:
#   macOS  → Apple Accelerate (always available, zero deps)
#   Linux  → OpenBLAS when present (-DHAVE_OPENBLAS); graceful scalar
#             fallback when not. Install: apt install libopenblas-dev
#   Other  → scalar fallback, all functionality preserved

CC      ?= cc
CFLAGS  ?= -O3 -march=native -Wall -Wextra -std=c11 -ffunction-sections -fdata-sections
CFLAGS += -I../BonfyreLex/src
CFLAGS  += -Iinclude
LDFLAGS ?= -lm
UNAME_S := $(shell uname -s)

ifeq ($(UNAME_S),Darwin)
  # Apple Silicon and Intel Mac: use bundled Accelerate framework
  CFLAGS  += -DACCELERATE_NEW_LAPACK
  LDFLAGS += -framework Accelerate
else ifeq ($(UNAME_S),Linux)
  # Linux: detect OpenBLAS via pkg-config, then header, then skip
  HAVE_OPENBLAS := $(shell pkg-config --exists openblas 2>/dev/null && echo yes || \
                           (echo "#include <cblas.h>" | $(CC) -x c -fsyntax-only - 2>/dev/null && echo yes) || \
                           echo no)
  ifeq ($(HAVE_OPENBLAS),yes)
    CFLAGS  += -DHAVE_OPENBLAS $(shell pkg-config --cflags openblas 2>/dev/null || echo "")
    LDFLAGS += $(shell pkg-config --libs openblas 2>/dev/null || echo "-lopenblas") -lgfortran -lpthread
  else
    # No OpenBLAS found — build compiles and runs, BLAS-accelerated
    # paths (large SVD, weight algebra) fall back to scalar C.
    # Install openblas for full speed: apt install libopenblas-dev
    $(info BonfyreFPQ: OpenBLAS not found — building with scalar BLAS fallback)
  endif
endif

SRC_DIR := src
INC_DIR := include
BUILD   := build

SRCS := $(SRC_DIR)/fwht.c \
        $(SRC_DIR)/polar.c \
        $(SRC_DIR)/seed.c \
        $(SRC_DIR)/qjl.c \
        $(SRC_DIR)/debruijn.c \
        $(SRC_DIR)/fpq_codec.c \
        $(SRC_DIR)/ggml_reader.c \
        $(SRC_DIR)/safetensors_reader.c \
        $(SRC_DIR)/serialize.c \
        $(SRC_DIR)/v4_optimizations.c \
        $(SRC_DIR)/weight_algebra.c \
        $(SRC_DIR)/fpq_native.c \
        $(SRC_DIR)/main.c

OBJS := $(patsubst $(SRC_DIR)/%.c,$(BUILD)/%.o,$(SRCS))
BIN  := bonfyre-fpq

.PHONY: all clean test test-serialization

all: $(BIN)

$(BIN): $(OBJS)
	$(CC) $(CFLAGS) -Wl,-dead_strip -o $@ $^ $(LDFLAGS)

$(BUILD)/%.o: $(SRC_DIR)/%.c $(INC_DIR)/fpq.h $(wildcard $(INC_DIR)/weight_algebra.h) | $(BUILD)
	$(CC) $(CFLAGS) -c -o $@ $<

$(BUILD):
	mkdir -p $(BUILD)

clean:
	rm -rf $(BUILD) $(BIN)

# Quick roundtrip test on a whisper model
../../lib/libbonfyre/libbonfyre.a:
	$(MAKE) -C ../../lib/libbonfyre

$(BUILD)/serialization-test: tests/serialization.c $(filter-out $(BUILD)/main.o,$(OBJS)) ../../lib/libbonfyre/libbonfyre.a
	$(CC) $(CFLAGS) -I../../lib/libbonfyre/include -o $@ $^ $(LDFLAGS)

$(BUILD)/serialization-faults: tests/serialization_faults.c src/serialize.c $(filter-out $(BUILD)/main.o $(BUILD)/serialize.o,$(OBJS))
	$(CC) $(CFLAGS) -o $@ tests/serialization_faults.c $(filter-out $(BUILD)/main.o $(BUILD)/serialize.o,$(OBJS)) $(LDFLAGS)

test-serialization: $(BUILD)/serialization-test $(BUILD)/serialization-faults
	$(abspath $(BUILD))/serialization-test tests/fixtures $(BUILD)/serialization-witness.json
	cue vet serialization.cue $(BUILD)/serialization-witness.json
	cue export serialization.cue tests/serialization_contract.cue -e '#File & #ContractCase' > /dev/null
	@for invalid in 'version: 5' 'flags: 2' 'version: 3' 'flags: 0' 'layout: "per-block"' \
	  'tensor_count: 2' 'tensors: [{dictionary_count: 2}]' 'tensors: [{block_count: 2}]' \
	  'tensors: [{mode: 2}]' 'tensors: [{blocks: [{dictionary_index: 1}]}]' \
	  'tensors: [{blocks: [{node_count: 4}]}]' 'tensors: [{blocks: [{qjl_words: 1}]}]'; do \
	  if cue export serialization.cue tests/serialization_contract.cue -e "#File & #ContractCase & {$$invalid}" > /dev/null 2>&1; then \
	    echo "CUE accepted malformed FPQ metadata: $$invalid"; exit 1; \
	  fi; \
	done
	@echo 'FPQ! CUE metadata contract: six native file witnesses and 12 refusal cases PASS'
	$(abspath $(BUILD))/serialization-faults tests/fixtures/legacy-v3.fpq

test: $(BIN) test-serialization
	@echo "=== FPQ Roundtrip Test ==="
	@if [ -f models/ggml-base.en.bin ]; then \
		./$(BIN) roundtrip models/ggml-base.en.bin --limit 5 --report --max-nodes 32 --tolerance 0.01; \
	else \
		echo "Skipping optional ggml roundtrip fixture (models/ggml-base.en.bin absent)"; \
	fi
	node tests/native_compression.test.mjs
