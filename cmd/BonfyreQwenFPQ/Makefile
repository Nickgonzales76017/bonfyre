CC ?= cc
comma := ,
UNAME_S := $(shell uname -s)

FPQ := ../BonfyreFPQ

CFLAGS ?= -O3 -march=native -Wall -Wextra -std=c11 -ffunction-sections -fdata-sections
CFLAGS += -Iinclude
CFLAGS += -I$(FPQ)/include
CFLAGS += -I$(FPQ)/legacy_from_10code/include
CFLAGS += -I../../lib/libbonfyre/include
CFLAGS += -D_POSIX_C_SOURCE=200809L
CFLAGS += -DFPQ_INT8_SDOT
CFLAGS += -I../BonfyreLex/src

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
  # Bake the shared-library search path into the binary. The QUIC stack lives in
  # its own prefix (OpenSSL 3.5 for the QUIC TLS API, which the distro's 3.0 does
  # not have), so without an rpath the loader falls back to the system libssl and
  # the binary dies at exec. Deriving it from pkg-config's -L keeps the two in
  # step instead of hardcoding a path that drifts.
  QUIC_LIBS += $(patsubst -L%,-Wl$(comma)-rpath$(comma)%,$(filter -L%,$(shell $(PKG_CONFIG) --libs-only-L libngtcp2 libngtcp2_crypto_ossl openssl)))
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

# make ORC=1 BIN=build/bonfyre-qwen-fpq-orc build/bonfyre-qwen-fpq-orc
# specialises the SLI kernel in-process through LLVM ORC (rules and the reason
# it is off by default are at the end of this file). Name the binary: plain
# `make ORC=1` runs `all`, which rebuilds the default binary linked against
# LLVM and installs it. That happened once and was put back.
ifeq ($(ORC),1)
  LLVM_PREFIX ?= $(shell for p in /opt/homebrew/opt/llvm /usr/local/opt/llvm /opt/local/libexec/llvm; do [ -x $$p/bin/llvm-config ] && echo $$p && break; done)
  SRC += src/fpq_orc_jit.c
  CFLAGS += -DFPQ_JIT_SLI -DFPQ_JIT_ORC -Ibuild -I$(shell $(LLVM_PREFIX)/bin/llvm-config --includedir)
  LDFLAGS += $(shell $(LLVM_PREFIX)/bin/llvm-config --ldflags) -lLLVM -Wl,-rpath,$(LLVM_PREFIX)/lib
endif

ifeq ($(UNAME_S),Darwin)
  SRC += src/fpq_metal_sli.m
  LDFLAGS += -framework Metal -framework Foundation
else
  SRC += src/fpq_metal_sli_stub.c
endif

HDR := $(wildcard include/*.h src/*.h) $(FPQ)/include/fpq.h

BIN := bonfyre-qwen-fpq
SYNC_BIN := ../../bin/bonfyre-qwen-fpq

all: $(BIN)

$(BIN): FORCE $(SRC) $(HDR) $(BONFYRE_LIB)
	$(CC) $(CFLAGS) -Wl,-dead_strip -o $@ $(SRC) $(BONFYRE_LIB) $(LDFLAGS) $(QUIC_LIBS)
	codesign -s - $@ >/dev/null 2>&1 || true

install-bin: $(BIN)
	@[ "$(ORC)" != 1 ] || { echo "refusing to install a binary linked against this machine's LLVM (ORC=1)" >&2; exit 1; }
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

tests/decode_row_shim_test: tests/decode_row_shim.test.c src/fpq_compat_shim.c src/fpq_active_cache.c src/fpq_kernels_neon.c $(BONFYRE_LIB)
	$(CC) $(CFLAGS) -o $@ tests/decode_row_shim.test.c src/fpq_compat_shim.c src/fpq_active_cache.c src/fpq_kernels_neon.c $(BONFYRE_LIB) $(LDFLAGS)

test: $(BIN) tests/decode_row_shim_test
	sh tests/attention_bias_contract.sh
	./tests/decode_row_shim_test

.PHONY: all clean install-bin test FORCE
FORCE:

# ── In-process kernel specialisation through LLVM ORC ───────────────────────
# Off unless asked for (make ORC=1): it links the LLVM on this machine, which
# a deployed binary should not need. The kernel's IR is made from its C source
# by that same LLVM, so the JIT always reads IR of its own version.
LLVM_PREFIX ?= $(shell for p in /opt/homebrew/opt/llvm /usr/local/opt/llvm /opt/local/libexec/llvm; do [ -x $$p/bin/llvm-config ] && echo $$p && break; done)
ORC_CFLAGS = -Ibuild $(filter-out -std=%,$(shell $(LLVM_PREFIX)/bin/llvm-config --cflags 2>/dev/null))
ORC_LIBS = $(shell $(LLVM_PREFIX)/bin/llvm-config --ldflags 2>/dev/null) -lLLVM -Wl,-rpath,$(LLVM_PREFIX)/lib

build/sli_jit_kernel_ir.h: src/sli_jit_kernel.c
	@mkdir -p build
	$(LLVM_PREFIX)/bin/clang -O1 -Xclang -disable-llvm-passes -S -emit-llvm -o build/sli_jit_kernel.ll src/sli_jit_kernel.c
	@{ echo '/* Generated from src/sli_jit_kernel.c by the Makefile; do not edit. */'; echo 'static const char FPQ_SLI_KERNEL_IR[] ='; \
	   sed -e 's/\\/\\\\/g' -e 's/"/\\"/g' -e 's/^/  "/' -e 's/$$/\\n"/' build/sli_jit_kernel.ll; echo ';'; } > $@

build/orc_jit_test: tests/orc_jit_test.c src/fpq_orc_jit.c include/fpq_orc_jit.h build/sli_jit_kernel_ir.h
	$(CC) -O3 -march=native -std=c11 -Iinclude $(ORC_CFLAGS) -o $@ tests/orc_jit_test.c src/fpq_orc_jit.c $(ORC_LIBS)

# Bit-identity of the specialised kernel against its source and against the
# temporary-C path it replaces, with what each costs. Needs LLVM; says so otherwise.
test-orc:
	@[ -n "$(LLVM_PREFIX)" ] || { echo "SKIP: no LLVM with llvm-config here"; exit 0; }; $(MAKE) build/orc_jit_test && ./build/orc_jit_test src/sli_jit_kernel.c
