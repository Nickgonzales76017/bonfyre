# bonfyre-vectors: one native runner for command vectors (PLAN.md W4).
#   make            build
#   make check      replay every ORGAN/tests/vectors/*.json in the repository
#
# Coverage (2026-10-29): every command of the 93-command plane in
# estate/legacy-operators.tsv has behavior vectors -- ORGAN/tests/vectors/command_behavior.json,
# BonfyreFinance's four files, and tests/vectors/plane/ for organs other agents own
# (BonfyreQueue: existing binary, read-only). Status per command, with the bugs the
# vectors found: packs/no-simulation/command-behavior-20261029.tsv.
# Needs locally: ffmpeg/ffprobe, macOS say, whisper ggml-base.en under
# ~/.local/share/whisper (symlinked read-only into the case), the Qwen2.5 tokenizer
# under ~/.bonfyre/representations (copied), FreeSWITCH (tel selftest), sqlite3,
# perl, curl. No case touches the network or writes outside its temp dir.
CC      ?= cc
LIB      = ../../lib/libbonfyre
CFLAGS  ?= -Os -Wall -Wextra -Werror -std=c11 -D_DEFAULT_SOURCE -D_DARWIN_C_SOURCE
CFLAGS  += -I$(LIB)/include
TARGET   = bonfyre-vectors
ROOT     = ../..

.PHONY: all check clean

all: $(TARGET)

$(LIB)/libbonfyre.a:
	$(MAKE) -C $(LIB)

$(TARGET): bf_vectors.c $(LIB)/libbonfyre.a
	$(CC) $(CFLAGS) bf_vectors.c $(LIB)/libbonfyre.a -lm -o $@

check: $(TARGET)
	cd $(ROOT) && tests/vectors/$(TARGET) --root . $$(find cmd components engine tests/vectors/plane -path '*/tests/vectors/*.json' -not -path '*/node_modules/*' | sort)

clean:
	rm -f $(TARGET)
