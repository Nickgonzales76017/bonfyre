CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
PROGRAM := bonfyre-showcase-estate
MODEL_JSON := generated/source-constellations.json
MODEL_HEADER := generated/source_constellations.h

.PHONY: all test clean native-witness

all: test

$(MODEL_JSON): source-constellations.cue
	mkdir -p generated
	cue export source-constellations.cue -e sourceConstellations --out json > $@

$(MODEL_HEADER): $(MODEL_JSON) ../BonfyreSurfaceMaturity/tools/embed_json.zig
	zig run ../BonfyreSurfaceMaturity/tools/embed_json.zig -- $(MODEL_JSON) $(MODEL_HEADER) bf_source_constellations_json

$(PROGRAM): src/main.c $(MODEL_HEADER)
	$(CC) $(CFLAGS) -Igenerated -o $@ src/main.c -lsqlite3

test: $(PROGRAM)
	sh tests/showcase_estate_test.sh ./$(PROGRAM)

clean:
	rm -f $(PROGRAM) $(MODEL_JSON) $(MODEL_HEADER)

native-witness: test
