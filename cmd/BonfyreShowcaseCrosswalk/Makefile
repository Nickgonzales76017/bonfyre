CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
PROGRAM := bonfyre-showcase-crosswalk
ESTATE ?= ../../estate/generated/external-estate-v4/observed-external-estate.jsonl

.PHONY: all deps test clean native-witness

all: test

deps:
	$(MAKE) -C ../BonfyreShowcaseEstate generated/source_constellations.h

$(PROGRAM): src/main.c deps
	$(CC) $(CFLAGS) -I../BonfyreShowcaseEstate/generated -o $@ src/main.c -lsqlite3

test: $(PROGRAM)
	sh tests/showcase_crosswalk_test.sh ./$(PROGRAM) $(ESTATE)

clean:
	rm -f $(PROGRAM)

native-witness: test
