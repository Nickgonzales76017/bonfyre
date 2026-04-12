# BonfyreFPQ — Functional Polar Quantization
# Store seed programs, not weights.

CC      := cc
CFLAGS  := -O3 -march=native -Wall -Wextra -std=c11 -Iinclude
LDFLAGS := -lm
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
  CFLAGS  += -DACCELERATE_NEW_LAPACK
  LDFLAGS += -framework Accelerate
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
