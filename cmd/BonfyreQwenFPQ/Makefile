CC ?= cc
UNAME_S := $(shell uname -s)

FPQ := ../BonfyreFPQ

CFLAGS ?= -O3 -march=native -Wall -Wextra -std=c11
CFLAGS += -Iinclude
CFLAGS += -I$(FPQ)/include
CFLAGS += -I$(FPQ)/legacy_from_10code/include
CFLAGS += -I../../lib/libbonfyre/include
CFLAGS += -D_POSIX_C_SOURCE=200809L
CFLAGS += -DFPQ_INT8_SDOT

BONFYRE_LIB := ../../lib/libbonfyre/libbonfyre.a
LDFLAGS ?= -lm -lpthread -lsqlite3
PKG_CONFIG := $(shell command -v pkg-config 2>/dev/null)
QUIC_SRC :=
QUIC_LIBS :=

ifneq ($(PKG_CONFIG),)
ifneq ($(shell $(PKG_CONFIG) --exists libngtcp2 libngtcp2_crypto_ossl openssl && echo yes),)
  CFLAGS += -DBF_HAS_QUIC
  CFLAGS += $(shell $(PKG_CONFIG) --cflags libngtcp2 libngtcp2_crypto_ossl openssl)
  CFLAGS += -I../../lib/libquic-transport/include
  QUIC_SRC += ../../lib/libquic-transport/bf_quic.c
  QUIC_LIBS += $(shell $(PKG_CONFIG) --libs libngtcp2 libngtcp2_crypto_ossl openssl)
endif
else ifneq ($(wildcard /opt/homebrew/lib/libngtcp2.dylib),)
  CFLAGS += -DBF_HAS_QUIC
  CFLAGS += -I/opt/homebrew/include -I../../lib/libquic-transport/include
  QUIC_SRC += ../../lib/libquic-transport/bf_quic.c
  QUIC_LIBS += -L/opt/homebrew/lib -lngtcp2 -lngtcp2_crypto_ossl -lssl -lcrypto
endif

ifeq ($(UNAME_S),Darwin)
  CFLAGS  += -DACCELERATE_NEW_LAPACK
  LDFLAGS += -framework Accelerate
endif

SRC := \
  src/main.c \
  src/fpq_run.c \
  src/tokenizer.c \
  src/fpqx_ops.c \
  src/fpq_kernels_neon.c \
  src/libfpq_sharded.c \
  src/fpq_native_read_compressed.c \
  src/qwen_runtime.c \
  src/qwen_serve.c \
  src/fpq_active_cache.c src/fpq_compat_shim.c \
  $(QUIC_SRC) \
  $(FPQ)/src/fwht.c \
  $(FPQ)/src/polar.c \
  $(FPQ)/src/seed.c \
  $(FPQ)/src/qjl.c \
  $(FPQ)/src/debruijn.c \
  $(FPQ)/src/fpq_codec_v2.c \
  $(FPQ)/src/ggml_reader.c \
  $(FPQ)/src/safetensors_reader.c \
  $(FPQ)/src/serialize.c \
  $(FPQ)/src/v4_optimizations.c \
  $(FPQ)/src/weight_algebra.c \
  $(FPQ)/legacy_from_10code/src/fpq_neon.c \
  $(FPQ)/src/fpq_native.c

ifeq ($(UNAME_S),Darwin)
  SRC += src/fpq_metal_sli.m
  LDFLAGS += -framework Metal -framework Foundation
else
  SRC += src/fpq_metal_sli_stub.c
endif

BIN := bonfyre-qwen-fpq
SYNC_BIN := ../../bin/bonfyre-qwen-fpq

all: $(BIN) install-bin

$(BIN): $(SRC)
	$(CC) $(CFLAGS) -o $@ $(SRC) $(BONFYRE_LIB) $(LDFLAGS) $(QUIC_LIBS)
	codesign -s - $@ >/dev/null 2>&1 || true

install-bin: $(BIN)
	mkdir -p ../../bin
	cp $(BIN) $(SYNC_BIN)
	codesign -s - $(SYNC_BIN) >/dev/null 2>&1 || true
	@# macOS hangs binaries executed from inside ~/Documents at dyld_start
	@# (TCC folder protection, unkillable) -- keep a runnable copy outside it.
	mkdir -p $(HOME)/.bonfyre/bin
	cp $(BIN) $(HOME)/.bonfyre/bin/$(BIN)
	codesign -s - $(HOME)/.bonfyre/bin/$(BIN) >/dev/null 2>&1 || true

clean:
	rm -f $(BIN) tests/decode_row_shim_test

tests/decode_row_shim_test: tests/decode_row_shim.test.c src/fpq_compat_shim.c
	$(CC) -Wall -Wextra -std=c11 -o $@ tests/decode_row_shim.test.c src/fpq_compat_shim.c

test: $(BIN) tests/decode_row_shim_test
	sh tests/attention_bias_contract.sh
	./tests/decode_row_shim_test

.PHONY: all clean install-bin test
