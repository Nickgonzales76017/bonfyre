# Bonfyre — top-level Makefile
# Builds all 38 binaries + liblambda-tensors library

PREFIX ?= $(HOME)/.local
BINDIR  = $(PREFIX)/bin
LIBDIR  = $(PREFIX)/lib
INCDIR  = $(PREFIX)/include

CC     ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11

# Every directory under cmd/ with a Makefile
BINARIES := $(sort $(dir $(wildcard cmd/*/Makefile)))

.PHONY: all lib binaries clean install test help

all: lib binaries

# ── Library ──────────────────────────────────────────────────
lib:
	@echo "=== Building liblambda-tensors ==="
	$(MAKE) -C lib/liblambda-tensors CC="$(CC)"

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
	@echo "Done. Ensure $(BINDIR) is in your PATH."

# ── Clean ────────────────────────────────────────────────────
clean:
	$(MAKE) -C lib/liblambda-tensors clean
	@for dir in $(BINARIES); do \
		$(MAKE) -C $$dir clean 2>/dev/null || true; \
	done
	@echo "Clean."

# ── Test ─────────────────────────────────────────────────────
test: all
	@echo "=== Running tests ==="
	$(MAKE) -C lib/liblambda-tensors test || true
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

# ── Help ─────────────────────────────────────────────────────
help:
	@echo "Bonfyre — 38 static C binaries, 1.6 MB total"
	@echo ""
	@echo "  make           Build everything"
	@echo "  make lib       Build liblambda-tensors only"
	@echo "  make install   Install to PREFIX (default: ~/.local)"
	@echo "  make clean     Remove all build artifacts"
	@echo "  make test      Run basic smoke tests"
	@echo "  make help      This message"
