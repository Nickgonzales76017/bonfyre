GIGATOKEN_SOURCE ?= $(HOME)/.bonfyre/substrates/v6.1/gigatoken/source
CARGO_TARGET_DIR ?= $(CURDIR)/gigatoken-native/target
# Keep tests incremental; the pinned upstream source is an explicit build input.
GIGATOKEN_INPUTS := $(shell find "$(GIGATOKEN_SOURCE)/src" -type f -name '*.rs' 2>/dev/null)

.PHONY: all test
all: bonfyre-gigatoken-native

bonfyre-gigatoken-native: gigatoken-native/Cargo.toml gigatoken-native/Cargo.lock \
		gigatoken-native/build.rs gigatoken-native/src/main.rs \
		$(GIGATOKEN_SOURCE)/Cargo.toml $(GIGATOKEN_INPUTS)
	GIGATOKEN_SOURCE="$(GIGATOKEN_SOURCE)" RUSTC_BOOTSTRAP=1 CARGO_TARGET_DIR="$(CARGO_TARGET_DIR)" cargo build --offline --locked --release --manifest-path gigatoken-native/Cargo.toml
	cp "$(CARGO_TARGET_DIR)/release/bonfyre-gigatoken-native" "$@"

test: bonfyre-gigatoken-native
	GIGATOKEN_BIN="$(CURDIR)/bonfyre-gigatoken-native" GIGATOKEN_TOKENIZER="$(GIGATOKEN_TOKENIZER)" sh tests/context_cut_test.sh context_cut.mjs
	GIGATOKEN_BIN="$(CURDIR)/bonfyre-gigatoken-native" GIGATOKEN_TOKENIZER="$(GIGATOKEN_TOKENIZER)" bun test tests/live_shadow_parity.test.mjs
