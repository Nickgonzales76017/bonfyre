# BonfyreFPQ — Functional Polar Quantization
# Store seed programs, not weights.

CC      := cc
CFLAGS  := -O3 -flto -march=native -Wall -Wextra -std=c11 -Iinclude -D_GNU_SOURCE
LDFLAGS := -flto -lm
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
  CFLAGS  += -DACCELERATE_NEW_LAPACK
  LDFLAGS += -framework Accelerate
  # OpenMP via Homebrew libomp — present = enable parallelism
  LIBOMP  := /opt/homebrew/opt/libomp
  ifneq ($(wildcard $(LIBOMP)/lib/libomp.dylib),)
    CFLAGS  += -Xclang -fopenmp -I$(LIBOMP)/include
    LDFLAGS += -L$(LIBOMP)/lib -lomp
  endif
else
  CFLAGS  += -DHAVE_OPENBLAS
  LDFLAGS += -lopenblas -llapack
  # OpenMP available natively on Linux GCC/Clang
  CFLAGS  += -fopenmp
  LDFLAGS += -fopenmp
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

# FPQ-X: Generalized Compression Algebra (A+M+Π+D+Λ+H)
FPQX_LIB_SRCS := $(SRC_DIR)/fwht.c \
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
                  $(SRC_DIR)/fpq_neon.c \
                  $(SRC_DIR)/fpqx_ops.c \
                  $(SRC_DIR)/fpqx_main.c

FPQX_OBJS := $(patsubst $(SRC_DIR)/%.c,$(BUILD)/%.o,$(FPQX_LIB_SRCS))
FPQX_BIN  := bonfyre-fpqx

.PHONY: all clean test fpqx fpq

all: $(BIN) $(FPQX_BIN) fpq

$(BIN): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

$(FPQX_BIN): $(FPQX_OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

fpqx: $(FPQX_BIN)

# ── Unified `fpq` binary (libfpq + SLI + NEON + CLI) ──
FPQ_CORE_SRCS := $(SRC_DIR)/fwht.c \
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
                 $(SRC_DIR)/fpqx_ops.c \
                 $(SRC_DIR)/fpq_neon.c \
                 $(SRC_DIR)/libfpq.c \
                 $(SRC_DIR)/tokenizer.c \
                 $(SRC_DIR)/fpq_run.c \
                 $(SRC_DIR)/fpq_cli.c

FPQ_CORE_OBJS := $(patsubst $(SRC_DIR)/%.c,$(BUILD)/%.o,$(FPQ_CORE_SRCS))
FPQ_BIN := fpq

fpq: $(FPQ_BIN)

$(FPQ_BIN): $(FPQ_CORE_OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

# ── libfpq shared library ──
LIBFPQ_SRCS := $(SRC_DIR)/fwht.c \
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
               $(SRC_DIR)/fpqx_ops.c \
               $(SRC_DIR)/fpq_neon.c \
               $(SRC_DIR)/libfpq.c

ifeq ($(UNAME_S),Darwin)
  LIBFPQ_EXT := dylib
  LIBFPQ_FLAGS := -dynamiclib
else
  LIBFPQ_EXT := so
  LIBFPQ_FLAGS := -shared -fPIC
endif

libfpq: libfpq.$(LIBFPQ_EXT)

libfpq.$(LIBFPQ_EXT): $(patsubst $(SRC_DIR)/%.c,$(BUILD)/%.o,$(LIBFPQ_SRCS))
	$(CC) $(CFLAGS) $(LIBFPQ_FLAGS) -o $@ $^ $(LDFLAGS)

$(BUILD)/%.o: $(SRC_DIR)/%.c $(INC_DIR)/fpq.h $(wildcard $(INC_DIR)/weight_algebra.h) $(wildcard $(INC_DIR)/fpqx.h) | $(BUILD)
	$(CC) $(CFLAGS) -c -o $@ $<

$(BUILD):
	mkdir -p $(BUILD)

clean:
	rm -rf $(BUILD) $(BIN) $(FPQX_BIN) $(FPQ_BIN) libfpq.dylib libfpq.so

# Quick roundtrip test on a whisper model
test: $(BIN)
	@echo "=== FPQ Roundtrip Test ==="
	./$(BIN) roundtrip models/ggml-base.en.bin --limit 5 --report --max-nodes 32 --tolerance 0.01

test-fpqx: $(FPQX_BIN)
	@echo "=== FPQ-X Roundtrip Test ==="
	./$(FPQX_BIN) roundtrip models/ggml-base.en.bin --limit 5 --bits 3
