CC ?= cc
CFLAGS ?= -O3 -std=c11 -ffp-contract=off -Wall -Wextra -Wpedantic -Werror

creator-mix: mix.c mix_kernel.c mix_kernel.h
	$(CC) $(CFLAGS) mix.c mix_kernel.c -lm -o $@

# WASM includes only the pure kernel; filesystem, effects and policy stay host-side.
creator-mix.wasm: mix_kernel.c mix_kernel.h
	clang --target=wasm32 -O3 -nostdlib -ffp-contract=off -Wl,--no-entry -Wl,--export=aurekai_mix_f32 -Wl,--export-memory mix_kernel.c -o $@

clean:
	rm -f creator-mix creator-mix.wasm
.PHONY: clean
