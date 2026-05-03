# Bonfyre — top-level Makefile
# Builds all Bonfyre binaries + liblambda-tensors + libbonfyre runtime

PREFIX ?= $(HOME)/.local
BINDIR  = $(PREFIX)/bin
LIBDIR  = $(PREFIX)/lib
INCDIR  = $(PREFIX)/include

CC     ?= cc
# MARCH controls the -march flag.
#   make             → -march=native  (local build, best perf for this CPU)
#   make MARCH=x86-64 → baseline x86-64 (portable, runs on any x86-64)
#   make MARCH=       → no -march flag  (let gcc choose; works on any arch)
MARCH  ?= native
ifneq ($(MARCH),)
  MARCH_FLAG := -march=$(MARCH)
else
  MARCH_FLAG :=
endif
# -D_DEFAULT_SOURCE exposes strdup, strndup, PATH_MAX and other POSIX/BSD
# extensions under glibc (-std=c11 alone hides them on Linux).
# Safe on macOS too — Apple libc ignores it harmlessly.
CFLAGS ?= -O3 $(MARCH_FLAG) -flto=auto -ffunction-sections -fdata-sections -Wall -Wextra -std=c11 -D_DEFAULT_SOURCE

# Every directory under cmd/ with a Makefile
BINARIES := $(sort $(dir $(wildcard cmd/*/Makefile)))

.PHONY: all lib binaries clean install test help sanitize fuzz docker docker-build dist \
	edge edge-esp32s3 edge-arm edge-arm64 edge-x86-64 \
	appliance-build runtime-pack model-memory-pack appliance-package manifest \
	appliance-smoke release-check release-notes

BUN_BIN      ?= $(HOME)/.bun/bin/bun
APPLIANCE_DIR ?= dist/bonfyre-appliance
HYPER_TARGET  ?= bun-linux-x64
BONFYRE_VERSION ?= 0.7.0
MANIFEST_SCHEMA ?= bonfyre.deploy.v1
RUNTIME_ABI     ?= bonfyre-abi-v1
MODEL_MEMORY_FAMILY ?= qwen3-8b
DATE_TAG        ?= $(shell date +%Y%m%d)

HYPER_BASENAME  ?= bonfyre-hyper-v$(BONFYRE_VERSION)-$(HYPER_TARGET)
HYPER_OUT       ?= dist/$(HYPER_BASENAME)
RUNTIME_PACK    ?= dist/bonfyre-runtime-v$(BONFYRE_VERSION)-$(HYPER_TARGET).tar.gz
RUNTIME_PACK_ZST ?= dist/bonfyre-runtime-v$(BONFYRE_VERSION)-$(HYPER_TARGET).tar.zst
MODEL_PACK      ?= dist/bonfyre-model-memory-$(MODEL_MEMORY_FAMILY)-$(DATE_TAG).tar.gz
MODEL_PACK_ZST  ?= dist/bonfyre-model-memory-$(MODEL_MEMORY_FAMILY)-$(DATE_TAG).tar.zst
APPLIANCE_TAR   ?= dist/bonfyre-appliance-v$(BONFYRE_VERSION)-$(HYPER_TARGET).tar.gz
APPLIANCE_TAR_ZST ?= dist/bonfyre-appliance-v$(BONFYRE_VERSION)-$(HYPER_TARGET).tar.zst

all: lib binaries

# ── Libraries ────────────────────────────────────────────────
lib:
	@echo "=== Building liblambda-tensors ==="
	$(MAKE) -C lib/liblambda-tensors CC="$(CC)" OPTFLAGS="$(CFLAGS)"
	@echo "=== Building libbonfyre ==="
	$(MAKE) -C lib/libbonfyre CC="$(CC)" OPTFLAGS="$(CFLAGS)"
	@echo "=== Building libquic-transport ==="
	$(MAKE) -C lib/libquic-transport CC="$(CC)" OPTFLAGS="$(CFLAGS)"

# ── Binaries ─────────────────────────────────────────────────
binaries: lib
	@total=0; ok=0; fail=0; \
	for dir in $(BINARIES); do \
		name=$$(basename $$dir); \
		printf "  [%2d] %-28s" $$((total+1)) "$$name"; \
		logfile=$$(mktemp); \
		if $(MAKE) -C $$dir CC="$(CC)" CFLAGS="$(CFLAGS)" > "$$logfile" 2>&1; then \
			echo "✓"; \
			ok=$$((ok+1)); \
		else \
			echo "✗"; \
			sed 's/^/      /' "$$logfile"; \
			fail=$$((fail+1)); \
		fi; \
		rm -f "$$logfile"; \
		total=$$((total+1)); \
	done; \
	echo ""; \
	echo "=== $$ok/$$total built ($$fail failed) ==="

# ── Install ──────────────────────────────────────────────────
install: all
	@mkdir -p $(BINDIR) $(LIBDIR) $(INCDIR)
	@echo "Installing to $(PREFIX)"
	@install -s -m 755 cmd/BonfyreCLI/bonfyre $(BINDIR)/ 2>/dev/null || true
	@for dir in $(BINARIES); do \
		name=$$(basename $$dir); \
		find "$$dir" -maxdepth 1 -name 'bonfyre-*' -type f -perm +111 \
			-exec install -s -m 755 {} $(BINDIR)/ \; 2>/dev/null; \
	done
	@cp lib/liblambda-tensors/liblambda-tensors.a $(LIBDIR)/ 2>/dev/null || true
	@cp lib/liblambda-tensors/liblambda-tensors.so $(LIBDIR)/ 2>/dev/null || true
	@cp lib/liblambda-tensors/include/lambda_tensors.h $(INCDIR)/ 2>/dev/null || true
	@cp lib/libbonfyre/libbonfyre.a $(LIBDIR)/ 2>/dev/null || true
	@cp lib/libbonfyre/include/bonfyre.h $(INCDIR)/ 2>/dev/null || true
	@cp lib/libbonfyre/include/bf_reactor.h $(INCDIR)/ 2>/dev/null || true
	@cp lib/libbonfyre/include/bf_hotload.h $(INCDIR)/ 2>/dev/null || true
	@cp lib/libquic-transport/libquic-transport.a $(LIBDIR)/ 2>/dev/null || true
	@cp lib/libquic-transport/include/bf_quic.h $(INCDIR)/ 2>/dev/null || true
	@echo "Done. Ensure $(BINDIR) is in your PATH."

# ── Clean ────────────────────────────────────────────────────
clean:
	$(MAKE) -C lib/liblambda-tensors clean
	$(MAKE) -C lib/libbonfyre clean
	$(MAKE) -C lib/libquic-transport clean 2>/dev/null || true
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

# ── Docker ────────────────────────────────────────────────────
.PHONY: docker docker-up docker-down
docker:
	docker build -t bonfyre .

# ── portable: build without march=native (runs on any CPU of this ISA) ────────
# Produces binaries safe for distribution across x86-64 or arm64 machines.
# Uses SIMD paths that the compiler auto-selects for the baseline ISA.
.PHONY: portable
portable:
	$(MAKE) MARCH= all

docker-up: docker
	docker compose up -d

docker-down:
	docker compose down

# ── docker-build: extract Linux x86_64 binaries from Docker image ────────────
# Use this from macOS to produce Linux-native binaries for distribution.
# Output: out/linux-$(DOCKER_ARCH)/  (default: linux-x86_64)
DOCKER_ARCH ?= x86_64
DOCKER_OUT  = out/linux-$(DOCKER_ARCH)

.PHONY: docker-build
docker-build:
	@echo "=== Building Linux $(DOCKER_ARCH) binaries via Docker ==="
	docker build --platform linux/$(DOCKER_ARCH) -t bonfyre-linux-$(DOCKER_ARCH) .
	@mkdir -p $(DOCKER_OUT)/bin $(DOCKER_OUT)/lib
	@id=$$(docker create --platform linux/$(DOCKER_ARCH) bonfyre-linux-$(DOCKER_ARCH)); \
	docker cp "$$id":/usr/local/bin/. $(DOCKER_OUT)/bin/ 2>/dev/null || true; \
	docker cp "$$id":/usr/local/lib/. $(DOCKER_OUT)/lib/ 2>/dev/null || true; \
	docker rm "$$id" > /dev/null
	@echo ""
	@echo "Linux $(DOCKER_ARCH) binaries extracted to $(DOCKER_OUT)/bin/"
	@ls -lhS $(DOCKER_OUT)/bin/ 2>/dev/null | head -20

# ── Edge / embedded cross-compilation ───────────────────────────────────────
# Builds libbonfyre.a for edge hardware profiles.
# Each subtarget cleans and rebuilds libbonfyre with the target compiler.
# Requires the appropriate cross-toolchain on PATH.
# Override compiler variables to point at your installed toolchain:
#   EDGE_ESP32S3_CC  (default: xtensa-esp32s3-elf-gcc  — from esp-idf / espressif toolchain)
#   EDGE_ARM_CC      (default: arm-linux-gnueabihf-gcc  — apt: gcc-arm-linux-gnueabihf)
#   EDGE_ARM64_CC    (default: aarch64-linux-gnu-gcc     — apt: gcc-aarch64-linux-gnu)
EDGE_ESP32S3_CC  ?= xtensa-esp32s3-elf-gcc
EDGE_ARM_CC      ?= arm-linux-gnueabihf-gcc
EDGE_ARM64_CC    ?= aarch64-linux-gnu-gcc
EDGE_COMMON_FLAGS = -O2 -std=c11 -D_DEFAULT_SOURCE -ffunction-sections -fdata-sections -Wall
EDGE_OUT          = out/edge

# 'make edge' runs all subtargets sequentially (each does a clean rebuild)
edge:
	$(MAKE) edge-esp32s3
	$(MAKE) edge-arm
	$(MAKE) edge-arm64
	$(MAKE) edge-x86-64

edge-esp32s3:
	@echo "=== Edge: XIAO ESP32-S3 Sense (Xtensa LX7) ==="
	$(MAKE) -C lib/libbonfyre clean
	$(MAKE) -C lib/libbonfyre \
		CC="$(EDGE_ESP32S3_CC)" \
		OPTFLAGS="$(EDGE_COMMON_FLAGS) -mlongcalls -mtext-section-literals -DESP32S3"
	@mkdir -p $(EDGE_OUT)/esp32s3
	@cp lib/libbonfyre/libbonfyre.a $(EDGE_OUT)/esp32s3/
	@cp lib/libbonfyre/include/bonfyre.h $(EDGE_OUT)/esp32s3/
	@echo "  → $(EDGE_OUT)/esp32s3/libbonfyre.a"

edge-arm:
	@echo "=== Edge: ARMv7-A hard-float (Adjusters field box, 32-bit ARM) ==="
	$(MAKE) -C lib/libbonfyre clean
	$(MAKE) -C lib/libbonfyre \
		CC="$(EDGE_ARM_CC)" \
		OPTFLAGS="$(EDGE_COMMON_FLAGS) -march=armv7-a -mfpu=neon-vfpv4 -mfloat-abi=hard"
	@mkdir -p $(EDGE_OUT)/arm
	@cp lib/libbonfyre/libbonfyre.a $(EDGE_OUT)/arm/
	@cp lib/libbonfyre/include/bonfyre.h $(EDGE_OUT)/arm/
	@echo "  → $(EDGE_OUT)/arm/libbonfyre.a"

edge-arm64:
	@echo "=== Edge: AArch64 / Cortex-A (Adjusters field box, 64-bit ARM) ==="
	$(MAKE) -C lib/libbonfyre clean
	$(MAKE) -C lib/libbonfyre \
		CC="$(EDGE_ARM64_CC)" \
		OPTFLAGS="$(EDGE_COMMON_FLAGS) -march=armv8-a"
	@mkdir -p $(EDGE_OUT)/arm64
	@cp lib/libbonfyre/libbonfyre.a $(EDGE_OUT)/arm64/
	@cp lib/libbonfyre/include/bonfyre.h $(EDGE_OUT)/arm64/
	@echo "  → $(EDGE_OUT)/arm64/libbonfyre.a"

edge-x86-64:
	@echo "=== Edge: Baseline x86-64 (portable, no native SIMD) ==="
	$(MAKE) -C lib/libbonfyre clean
	$(MAKE) -C lib/libbonfyre \
		CC="$(CC)" \
		OPTFLAGS="$(EDGE_COMMON_FLAGS) -march=x86-64"
	@mkdir -p $(EDGE_OUT)/x86-64
	@cp lib/libbonfyre/libbonfyre.a $(EDGE_OUT)/x86-64/
	@cp lib/libbonfyre/include/bonfyre.h $(EDGE_OUT)/x86-64/
	@echo "  → $(EDGE_OUT)/x86-64/libbonfyre.a"

# ── Help ─────────────────────────────────────────────────────
help:
	@echo "Bonfyre — Bonfyre binary fleet + 2 libraries, ~2.1 MB total"
	@echo ""
	@echo "  make           Build everything (optimized for this CPU)"
	@echo "  make portable  Build without -march=native (distributable binaries)"
	@echo "  make MARCH=x86-64 all  Build for baseline x86-64 (no native tuning)"
	@echo "  make lib       Build liblambda-tensors + libbonfyre"
	@echo "  make install   Install to PREFIX (default: ~/.local)"
	@echo "  make models    Download required ML models"
	@echo "  make clean     Remove all build artifacts"
	@echo "  make test      Run all test suites"
	@echo "  make sanitize  Rebuild with ASan + UBSan for testing"
	@echo "  make pgo-gen   Build with profiling instrumentation"
	@echo "  make pgo-use   Rebuild using collected profile data"
	@echo "  make pgo-clean Remove collected profile data"
	@echo ""
	@echo "Appliance deployment targets:"
	@echo "  make appliance-build HYPER_TARGET=bun-linux-x64"
	@echo "  make runtime-pack    Build native runtime pack tarball"
	@echo "  make model-memory-pack Build model-memory pack tarball"
	@echo "  make manifest        Emit bonfyre.manifest.json"
	@echo "  make appliance-package Build complete appliance directory + tarball"
	@echo "  make appliance-smoke  Smoke test compiled hyper deployment"
	@echo "  make release-check    Full release gate (fails if deep doctor fails)"
	@echo "  make release-notes    Generate release notes from manifest"

# ── Appliance deployment (Bun + runtime packs) ─────────────────────────────
appliance-build:
	@echo "=== Compiling bonfyre-hyper ($(HYPER_TARGET)) ==="
	@mkdir -p dist
	@cd bonfyre-hyper && $(BUN_BIN) build ./src/hyper.ts --compile --target=$(HYPER_TARGET) --outfile ../$(HYPER_OUT)
	@chmod +x $(HYPER_OUT)
	@echo "  → $(HYPER_OUT)"

runtime-pack:
	@echo "=== Building runtime pack ($(RUNTIME_PACK)) ==="
	@echo "  (packaging existing built binaries; run 'make binaries' first for a fresh runtime)"
	@mkdir -p dist
	@staging=$$(mktemp -d /tmp/bonfyre-runtime.XXXXXX); \
	trap 'rm -rf "$$staging"' EXIT; \
	mkdir -p "$$staging/bin" "$$staging/lib" "$$staging/runtime/recipes" "$$staging/runtime/schemas"; \
	cp cmd/BonfyreCLI/bonfyre "$$staging/bin/" 2>/dev/null || true; \
	for d in $(BINARIES); do \
		find "$$d" -maxdepth 1 -type f -name 'bonfyre*' -perm -111 -exec cp {} "$$staging/bin/" \; 2>/dev/null || true; \
	done; \
	cp lib/libbonfyre/libbonfyre.dylib "$$staging/lib/" 2>/dev/null || true; \
	cp lib/libbonfyre/libbonfyre.so "$$staging/lib/" 2>/dev/null || true; \
	tar -cf - --exclude='auto/cross_gen' -C recipes . 2>/dev/null | tar -xf - -C "$$staging/runtime/recipes"; \
	if [ -d schemas ]; then tar -cf - -C schemas . 2>/dev/null | tar -xf - -C "$$staging/runtime/schemas"; fi; \
	printf '{\n  "schema_version": "$(MANIFEST_SCHEMA)",\n  "runtime_abi": "$(RUNTIME_ABI)",\n  "kind": "bonfyre-runtime-schema-snapshot"\n}\n' > "$$staging/runtime/schemas/bonfyre-runtime.json"; \
	tmp_tar=$$(mktemp /tmp/bonfyre-runtime.XXXXXX.tar); \
	tar -cf "$$tmp_tar" -C "$$staging" .; \
	gzip -c "$$tmp_tar" > $(RUNTIME_PACK); \
	if command -v zstd >/dev/null 2>&1; then zstd -q -f "$$tmp_tar" -o $(RUNTIME_PACK_ZST); fi; \
	rm -f "$$tmp_tar"
	@echo "  → $(RUNTIME_PACK)"
	@if [ -f $(RUNTIME_PACK_ZST) ]; then echo "  → $(RUNTIME_PACK_ZST)"; fi

model-memory-pack:
	@echo "=== Building model-memory pack ($(MODEL_PACK)) ==="
	@mkdir -p dist
	@staging=$$(mktemp -d /tmp/bonfyre-model.XXXXXX); \
	trap 'rm -rf "$$staging"' EXIT; \
	cp model-memory/*.bfmodel "$$staging/" 2>/dev/null || true; \
	cp model-memory/*.akmodel "$$staging/" 2>/dev/null || true; \
	cp model-memory/*.bfsae  "$$staging/" 2>/dev/null || true; \
	cp model-memory/*.aksae  "$$staging/" 2>/dev/null || true; \
	cp model-memory/*.bffpqx "$$staging/" 2>/dev/null || true; \
	cp model-memory/*.akfpqx "$$staging/" 2>/dev/null || true; \
	cp model-memory/*.bfembed "$$staging/" 2>/dev/null || true; \
	cp model-memory/*.akembed "$$staging/" 2>/dev/null || true; \
	cp model-memory/*.bfcache "$$staging/" 2>/dev/null || true; \
	cp model-memory/*.akcache "$$staging/" 2>/dev/null || true; \
	if [ ! -f "$$staging/default.bfsae" ] && [ -f "$$HOME/.local/share/bonfyre/sae/default.bfsae" ]; then cp "$$HOME/.local/share/bonfyre/sae/default.bfsae" "$$staging/default.bfsae"; fi; \
	if [ ! -f "$$staging/default.aksae" ] && [ -f "$$HOME/.local/share/bonfyre/sae/default.aksae" ]; then cp "$$HOME/.local/share/bonfyre/sae/default.aksae" "$$staging/default.aksae"; fi; \
	if [ ! -f "$$staging/$(MODEL_MEMORY_FAMILY).bfmodel" ]; then \
		printf '{\n  "artifact_type": "bonfyre-model-pack",\n  "model_id": "$(MODEL_MEMORY_FAMILY)-default",\n  "model_family": "$(MODEL_MEMORY_FAMILY)",\n  "weights": "weights/$(MODEL_MEMORY_FAMILY).gguf",\n  "tokenizer": "tokenizer.json",\n  "sae_dictionaries": ["default.bfsae"],\n  "feature_policies": "policies/default.bffeaturepolicy",\n  "embedding_policy": "embed/default.bfembed",\n  "cache_policy": "cache/default.bfcache"\n}\n' > "$$staging/$(MODEL_MEMORY_FAMILY).bfmodel"; \
	fi; \
	if [ ! -f "$$staging/default.bfsae" ] && [ -f "$$staging/default.aksae" ]; then cp "$$staging/default.aksae" "$$staging/default.bfsae"; fi; \
	if [ ! -f "$$staging/default.aksae" ] && [ -f "$$staging/default.bfsae" ]; then cp "$$staging/default.bfsae" "$$staging/default.aksae"; fi; \
	if [ ! -f "$$staging/$(MODEL_MEMORY_FAMILY).akmodel" ] && [ -f "$$staging/$(MODEL_MEMORY_FAMILY).bfmodel" ]; then \
		sed 's/\.bfsae/\.aksae/g; s/\.bffeaturepolicy/\.akfeaturepolicy/g; s/\.bfembed/\.akembed/g; s/\.bfcache/\.akcache/g' \
			"$$staging/$(MODEL_MEMORY_FAMILY).bfmodel" > "$$staging/$(MODEL_MEMORY_FAMILY).akmodel"; \
	fi; \
	tmp_tar=$$(mktemp /tmp/bonfyre-model.XXXXXX.tar); \
	tar -cf "$$tmp_tar" -C "$$staging" .; \
	gzip -c "$$tmp_tar" > $(MODEL_PACK); \
	if command -v zstd >/dev/null 2>&1; then zstd -q -f "$$tmp_tar" -o $(MODEL_PACK_ZST); fi; \
	rm -f "$$tmp_tar"
	@echo "  → $(MODEL_PACK)"
	@if [ -f $(MODEL_PACK_ZST) ]; then echo "  → $(MODEL_PACK_ZST)"; fi

manifest:
	@echo "=== Writing bonfyre.manifest.json ==="
	@mkdir -p dist
	@python3 scripts/gen_manifest.py \
		--target $(HYPER_TARGET) \
		--out bonfyre.manifest.json \
		--release $(BONFYRE_VERSION) \
		--schema $(MANIFEST_SCHEMA) \
		--runtime-abi $(RUNTIME_ABI) \
		--default-model-family $(MODEL_MEMORY_FAMILY)
	@cp bonfyre.manifest.json dist/bonfyre.manifest.json

appliance-package: appliance-build runtime-pack model-memory-pack manifest
	@echo "=== Assembling appliance directory ==="
	@rm -rf $(APPLIANCE_DIR)
	@mkdir -p $(APPLIANCE_DIR)/bin $(APPLIANCE_DIR)/lib $(APPLIANCE_DIR)/runtime $(APPLIANCE_DIR)/model-memory $(APPLIANCE_DIR)/var
	@cp $(HYPER_OUT) $(APPLIANCE_DIR)/bin/bonfyre-hyper
	@tar -xzf $(RUNTIME_PACK) -C $(APPLIANCE_DIR)
	@if [ -f $(MODEL_PACK) ]; then tar -xzf $(MODEL_PACK) -C $(APPLIANCE_DIR)/model-memory; fi
	@cp bonfyre.manifest.json $(APPLIANCE_DIR)/runtime/bonfyre.manifest.json
	@mkdir -p dist
	@tmp_tar=$$(mktemp /tmp/bonfyre-appliance.XXXXXX.tar); \
	tar -cf "$$tmp_tar" -C dist bonfyre-appliance; \
	gzip -c "$$tmp_tar" > $(APPLIANCE_TAR); \
	if command -v zstd >/dev/null 2>&1; then zstd -q -f "$$tmp_tar" -o $(APPLIANCE_TAR_ZST); fi; \
	rm -f "$$tmp_tar"
	@echo "  → $(APPLIANCE_TAR)"
	@if [ -f $(APPLIANCE_TAR_ZST) ]; then echo "  → $(APPLIANCE_TAR_ZST)"; fi

appliance-smoke: appliance-build manifest
	@echo "=== Appliance smoke test ==="
	@BONFYRE_RUNTIME=$(CURDIR)/dist/bonfyre-appliance \
	BONFYRE_HOME=/tmp/bonfyre-smoke-state \
	$(HYPER_OUT) help > /tmp/bonfyre-hyper-help.txt
	@BONFYRE_RUNTIME=$(CURDIR)/dist/bonfyre-appliance \
	BONFYRE_HOME=/tmp/bonfyre-smoke-state \
	$(HYPER_OUT) doctor --deep --manifest dist/bonfyre.manifest.json > /tmp/bonfyre-hyper-doctor.txt
	@echo "  help:   /tmp/bonfyre-hyper-help.txt"
	@echo "  doctor: /tmp/bonfyre-hyper-doctor.txt"

release-check:
	$(MAKE) manifest
	$(MAKE) runtime-pack
	$(MAKE) model-memory-pack
	$(MAKE) appliance-build HYPER_TARGET=$(HYPER_TARGET)
	$(MAKE) appliance-package HYPER_TARGET=$(HYPER_TARGET)
	BONFYRE_RUNTIME=$(CURDIR)/dist/bonfyre-appliance \
	BONFYRE_HOME=/tmp/bonfyre-release-check-state \
	$(HYPER_OUT) doctor --deep --manifest dist/bonfyre.manifest.json

release-notes: manifest
	@python3 scripts/release_notes_from_manifest.py --manifest dist/bonfyre.manifest.json --out dist/release-notes.md
	@echo "  → dist/release-notes.md"
# ── dist: source tarball ─────────────────────────────────────────────────────
# Whitelist-based: only explicitly listed paths are included.
# This prevents workspace state (Obsidian vault, .bonfyre-runtime, layeros DB,
# site/demos audio, *.db, etc.) from creeping into the dist.
# Output: ~/Downloads/bonfyre-YYYYMMDD.tar.gz
DIST_NAME   ?= bonfyre-$(shell date +%Y%m%d)
DIST_DEST   ?= $(HOME)/Downloads/$(DIST_NAME).tar.gz

.PHONY: dist
dist:
	@echo "=== Creating source distribution: $(DIST_DEST) ==="
	tar -czf "$(DIST_DEST)" \
		--exclude='.git' \
		--exclude='*.o' \
		--exclude='*.a' \
		--exclude='*.db' \
		--exclude='*.sqlite' \
		--exclude='*.wav' \
		--exclude='*.mp3' \
		--exclude='*.m4a' \
		--exclude='*/build' \
		--exclude='out/' \
		--exclude='cmd/BonfyreFPQ/legacy_from_10code' \
		cmd/ lib/ scripts/ docs/ \
		Makefile install.sh README.md LICENSE \
		Dockerfile docker-compose.yml
	@echo ""
	@echo "Created: $(DIST_DEST)  ($$(du -sh "$(DIST_DEST)" | cut -f1))"
	@echo "Contents breakdown:"
	@tar -tzf "$(DIST_DEST)" | awk -F/ 'NF>=2{c[$$2]++} END{for(d in c) print c[d], d}' | sort -rn | head -15
	@echo "  Linux build: tar xf $(DIST_NAME).tar.gz && make CC=gcc"


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
