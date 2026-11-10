CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
PROGRAM := bonfyre-observation-boundary
CONTRACT_JSON := generated/boundary-contract.json
CONTRACT_HEADER := generated/observation_boundary_contract.h

.PHONY: all clean test

all: test

$(CONTRACT_JSON): boundary-contract.cue
	mkdir -p generated
	cue export boundary-contract.cue -e boundaryContract --out json > $@

$(CONTRACT_HEADER): $(CONTRACT_JSON) tools/compile_boundary_contract.zig
	zig run tools/compile_boundary_contract.zig -- $(CONTRACT_JSON) $(CONTRACT_HEADER)

$(PROGRAM): src/main.c $(CONTRACT_HEADER)
	$(CC) $(CFLAGS) -Igenerated -o $@ src/main.c -lsqlite3

test: $(PROGRAM)
	sh tests/observation_boundary_test.sh ./$(PROGRAM)

clean:
	rm -f $(PROGRAM) $(CONTRACT_JSON) $(CONTRACT_HEADER)
