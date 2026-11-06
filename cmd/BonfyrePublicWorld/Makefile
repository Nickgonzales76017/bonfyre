CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
PROGRAM := bonfyre-public-world

.PHONY: all deps test clean native-witness

all: test

deps:
	$(MAKE) -C ../BonfyreSurfaceMaturity generated/surface_maturity.h
	$(MAKE) -C ../BonfyreShowcaseEstate generated/source_constellations.h

$(PROGRAM): src/main.c deps
	$(CC) $(CFLAGS) -I../BonfyreSurfaceMaturity/generated -I../BonfyreShowcaseEstate/generated \
	  -o $@ src/main.c -lsqlite3

test: $(PROGRAM)
	sh tests/public_world_test.sh ./$(PROGRAM)

clean:
	rm -f $(PROGRAM)

native-witness: test
