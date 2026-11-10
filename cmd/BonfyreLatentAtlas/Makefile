CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
ROOT := ../..
TARGET = bonfyre-latent-atlas

all: $(TARGET)

$(TARGET): src/main.c $(ROOT)/lib/libbonfyre/include/bf_command_lookup.h $(ROOT)/lib/libbonfyre/bf_json.c $(ROOT)/lib/libbonfyre/src/bf_sha256.c
	$(CC) $(CFLAGS) -I$(ROOT)/lib/libbonfyre/include -o $@ src/main.c $(ROOT)/lib/libbonfyre/bf_json.c $(ROOT)/lib/libbonfyre/src/bf_sha256.c -lsqlite3 -lm

test: $(TARGET)
	./tests/latent_atlas_test.sh ./$(TARGET)
	./tests/link_music_identities_test.sh
	sh ./tests/link_identities_native_test.sh
	bun test ./tests/audio_sample_test.mjs ./tests/compile_acoustic_sweep_test.mjs

clean:
	rm -f $(TARGET)

.PHONY: all test clean
