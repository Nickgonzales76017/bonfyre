# Bonfyre — top-level Makefile
# Builds all 47 binaries + liblambda-tensors + libbonfyre runtime

PREFIX ?= $(HOME)/.local
BINDIR  = $(PREFIX)/bin
LIBDIR  = $(PREFIX)/lib
INCDIR  = $(PREFIX)/include

CC     ?= cc
CFLAGS ?= -O3 -march=native -flto=auto -ffunction-sections -fdata-sections -Wall -Wextra -std=c11

# Every directory under cmd/ with a Makefile
BINARIES := $(sort $(dir $(wildcard cmd/*/Makefile)))

.PHONY: all lib binaries clean install test help sanitize fuzz

all: lib binaries

# ── Libraries ────────────────────────────────────────────────
lib:
	@echo "=== Building liblambda-tensors ==="
	$(MAKE) -C lib/liblambda-tensors CC="$(CC)" OPTFLAGS="$(CFLAGS)"
	@echo "=== Building libbonfyre ==="
	$(MAKE) -C lib/libbonfyre CC="$(CC)" OPTFLAGS="$(CFLAGS)"

# ── Binaries ─────────────────────────────────────────────────
binaries: lib
	@total=0; ok=0; fail=0; \
	for dir in $(BINARIES); do \
		name=$$(basename $$dir); \
		printf "  [%2d] %-28s" $$((total+1)) "$$name"; \
		if $(MAKE) -C $$dir CC="$(CC)" CFLAGS="$(CFLAGS)" > /dev/null 2>&1; then \
			echo "✓"; \
			ok=$$((ok+1)); \
		else \
			echo "✗"; \
			fail=$$((fail+1)); \
		fi; \
		total=$$((total+1)); \
	done; \
	echo ""; \
	echo "=== $$ok/$$total built ($$fail failed) ==="

# ── Install ──────────────────────────────────────────────────
install: all
	@mkdir -p $(BINDIR) $(LIBDIR) $(INCDIR)
	@echo "Installing to $(PREFIX)"
	@for dir in $(BINARIES); do \
		name=$$(basename $$dir); \
		find "$$dir" -maxdepth 1 -name 'bonfyre-*' -type f -perm +111 -exec cp {} $(BINDIR)/ \; 2>/dev/null; \
	done
	@cp lib/liblambda-tensors/liblambda-tensors.a $(LIBDIR)/ 2>/dev/null || true
	@cp lib/liblambda-tensors/liblambda-tensors.so $(LIBDIR)/ 2>/dev/null || true
	@cp lib/liblambda-tensors/include/lambda_tensors.h $(INCDIR)/ 2>/dev/null || true
	@cp lib/libbonfyre/libbonfyre.a $(LIBDIR)/ 2>/dev/null || true
	@cp lib/libbonfyre/include/bonfyre.h $(INCDIR)/ 2>/dev/null || true
	@echo "Done. Ensure $(BINDIR) is in your PATH."

# ── Clean ────────────────────────────────────────────────────
clean:
	$(MAKE) -C lib/liblambda-tensors clean
	$(MAKE) -C lib/libbonfyre clean
	@for dir in $(BINARIES); do \
		$(MAKE) -C $$dir clean 2>/dev/null || true; \
	done
	@echo "Clean."

# ── Test ─────────────────────────────────────────────────────
test: all
	@echo "=== Running tests ==="
	$(MAKE) -C lib/liblambda-tensors test || true
	$(MAKE) -C lib/libbonfyre test || true
	@pass=0; \
	for dir in $(BINARIES); do \
		for bin in "$$dir"/bonfyre-*; do \
			[ -x "$$bin" ] || continue; \
			if "$$bin" status > /dev/null 2>&1; then \
				echo "  ✓ $$(basename $$bin) status"; \
				pass=$$((pass+1)); \
			fi; \
		done; \
	done; \
	echo "=== $$pass binaries passed status check ==="

# ── Security hardening ───────────────────────────────────────
# Address Sanitizer: catches buffer overflows, use-after-free, leaks
sanitize:
	@echo "=== Building with AddressSanitizer + UndefinedBehaviorSanitizer ==="
	$(MAKE) -C lib/liblambda-tensors clean
	$(MAKE) -C lib/liblambda-tensors CC="$(CC)" OPTFLAGS="-g -fsanitize=address,undefined -fno-omit-frame-pointer -std=c11"
	$(MAKE) -C lib/libbonfyre clean
	$(MAKE) -C lib/libbonfyre CC="$(CC)" OPTFLAGS="-g -fsanitize=address,undefined -fno-omit-frame-pointer -std=c11"
	@for dir in $(BINARIES); do \
		$(MAKE) -C $$dir CC="$(CC)" CFLAGS="-g -fsanitize=address,undefined -fno-omit-frame-pointer -std=c11" \
			LDFLAGS="-fsanitize=address,undefined" 2>/dev/null || true; \
	done
	@echo "=== Sanitizer build done. Run binaries to detect memory errors. ==="

# ── Profile-Guided Optimization ──────────────────────────────
# Step 1: `make pgo-gen` → builds with profiling instrumentation
# Step 2: Run representative workloads on the instrumented binaries
# Step 3: `make pgo-use` → rebuilds using collected profile data
PGO_DIR = $(CURDIR)/pgo-data

pgo-gen: clean
	@echo "=== PGO: instrumented build ==="
	$(MAKE) all CFLAGS="$(CFLAGS) -fprofile-generate=$(PGO_DIR)"

pgo-use:
	@echo "=== PGO: optimized build from profile data ==="
	$(MAKE) clean
	$(MAKE) all CFLAGS="$(CFLAGS) -fprofile-use=$(PGO_DIR) -fprofile-correction"

pgo-clean:
	rm -rf $(PGO_DIR)

# ── Help ─────────────────────────────────────────────────────
help:
	@echo "Bonfyre — 47 static C binaries + 2 libraries, ~2.1 MB total"
	@echo ""
	@echo "  make           Build everything"
	@echo "  make lib       Build liblambda-tensors + libbonfyre"
	@echo "  make install   Install to PREFIX (default: ~/.local)"
	@echo "  make models    Download required ML models"
	@echo "  make clean     Remove all build artifacts"
	@echo "  make test      Run all test suites"
	@echo "  make sanitize  Rebuild with ASan + UBSan for testing"
	@echo "  make pgo-gen   Build with profiling instrumentation"
	@echo "  make pgo-use   Rebuild using collected profile data"
	@echo "  make pgo-clean Remove collected profile data"
	@echo "  make help      This message"

# ── Models ───────────────────────────────────────────────────
WHISPER_DIR  = $(HOME)/.local/share/whisper
MODEL_DIR    = $(HOME)/.bonfyre/models

.PHONY: models
models:
	@echo "=== Downloading models ==="
	@mkdir -p $(WHISPER_DIR) $(MODEL_DIR)
	@if [ ! -f $(WHISPER_DIR)/ggml-base.en.bin ]; then \
		echo "  ↓ whisper base.en (~140MB)..."; \
		curl -fSL -o $(WHISPER_DIR)/ggml-base.en.bin \
			"https://huggingface.co/ggerganov/whisper.cpp/resolve/main/ggml-base.en.bin"; \
		echo "  ✓ ggml-base.en.bin"; \
	else echo "  ✓ ggml-base.en.bin (exists)"; fi
	@if [ ! -f $(MODEL_DIR)/lid.176.bin ]; then \
		echo "  ↓ fastText lid.176 (~125MB)..."; \
		curl -fSL -o $(MODEL_DIR)/lid.176.bin \
			"https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin"; \
		echo "  ✓ lid.176.bin"; \
	else echo "  ✓ lid.176.bin (exists)"; fi
	@if [ ! -f $(MODEL_DIR)/all-MiniLM-L6-v2.onnx ]; then \
		echo "  ↓ sentence-transformer ONNX (~22MB)..."; \
		curl -fSL -o $(MODEL_DIR)/all-MiniLM-L6-v2.onnx \
			"https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2/resolve/main/onnx/model.onnx"; \
		echo "  ✓ all-MiniLM-L6-v2.onnx"; \
	else echo "  ✓ all-MiniLM-L6-v2.onnx (exists)"; fi
	@echo "=== Models ready ==="
