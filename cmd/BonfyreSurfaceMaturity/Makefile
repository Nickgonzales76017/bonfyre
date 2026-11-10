CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
PROGRAM := bonfyre-surface-maturity
TARGET := bonfyre-surface-maturity
MODEL_JSON := generated/surface-maturity.json
MODEL_HEADER := generated/surface_maturity.h

.PHONY: all test clean native-witness

all: test

$(MODEL_JSON): surface-maturity.cue
	mkdir -p generated
	cue export surface-maturity.cue -e surfaceMaturity --out json > $@

$(MODEL_HEADER): $(MODEL_JSON) tools/embed_json.zig
	zig run tools/embed_json.zig -- $(MODEL_JSON) $(MODEL_HEADER) bf_surface_maturity_json

$(PROGRAM): src/main.c $(MODEL_HEADER)
$(PROGRAM): src/account_maturity_print.h
	$(CC) $(CFLAGS) -Igenerated -o $@ src/main.c -lsqlite3

test: $(PROGRAM)
	sh tests/surface_maturity_test.sh ./$(PROGRAM)

clean:
	rm -f $(PROGRAM) $(MODEL_JSON) $(MODEL_HEADER)

native-witness: test
