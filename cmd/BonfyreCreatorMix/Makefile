CC ?= cc
CFLAGS ?= -O3 -std=c11 -Wall -Wextra -Wpedantic -Werror
SOURCE := ../../Aurekai/native/creator
TARGET := bonfyre-creator-mix

all: $(TARGET)

# Preserve ordered float32 accumulation and nonfinite checks even if a parent
# build passes tuning flags. This kernel is also the browser WASM source.
$(TARGET): $(SOURCE)/mix.c $(SOURCE)/mix_kernel.c $(SOURCE)/mix_kernel.h
	$(CC) $(CFLAGS) -fno-fast-math -ffp-contract=off $(SOURCE)/mix.c $(SOURCE)/mix_kernel.c -lm -o $@

wasm:
	node tools/wasm.mjs --build

verify-wasm:
	node tools/wasm.mjs

test: $(TARGET) verify-wasm
	node --test ../../Aurekai/tests/creator-render-program.test.mjs ../../Aurekai/tests/creator-native-pcm.test.mjs ../../Aurekai/tests/creator-native-mix.test.mjs ../../Aurekai/tests/creator-cli.test.mjs ../../Aurekai/tests/creator-wasm.test.mjs

clean:
	rm -f $(TARGET)

.PHONY: all test clean wasm verify-wasm
