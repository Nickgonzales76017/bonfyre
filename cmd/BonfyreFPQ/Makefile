# BonfyreFPQ — Functional Polar Quantization
# Store seed programs, not weights.
#
# Platform BLAS strategy:
#   macOS  → Apple Accelerate (always available, zero deps)
#   Linux  → OpenBLAS when present (-DHAVE_OPENBLAS); graceful scalar
#             fallback when not. Install: apt install libopenblas-dev
#   Other  → scalar fallback, all functionality preserved

CC      ?= cc
CFLAGS  ?= -O3 -march=native -Wall -Wextra -std=c11
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

.PHONY: all clean test

all: $(BIN)

$(BIN): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

$(BUILD)/%.o: $(SRC_DIR)/%.c $(INC_DIR)/fpq.h $(wildcard $(INC_DIR)/weight_algebra.h) | $(BUILD)
	$(CC) $(CFLAGS) -c -o $@ $<

$(BUILD):
	mkdir -p $(BUILD)

clean:
	rm -rf $(BUILD) $(BIN)

# Quick roundtrip test on a whisper model
test: $(BIN)
	@echo "=== FPQ Roundtrip Test ==="
	./$(BIN) roundtrip models/ggml-base.en.bin --limit 5 --report --max-nodes 32 --tolerance 0.01
