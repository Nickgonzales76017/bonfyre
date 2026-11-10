CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
PROGRAM := bonfyre-bridge-readiness
CONTRACT_JSON := generated/readiness-contract.json
CONTRACT_HEADER := generated/readiness_contract.h

.PHONY: all clean test

all: test

$(CONTRACT_JSON): readiness-contract.cue
	mkdir -p generated
	cue export readiness-contract.cue -e readinessContract --out json > $@

$(CONTRACT_HEADER): $(CONTRACT_JSON) tools/compile_readiness_contract.zig
	zig run tools/compile_readiness_contract.zig -- $(CONTRACT_JSON) $(CONTRACT_HEADER)

$(PROGRAM): src/main.c $(CONTRACT_HEADER)
	$(CC) $(CFLAGS) -Igenerated -o $@ src/main.c -lsqlite3

test: $(PROGRAM)
	@if [ -f tests/bridge_readiness_test.sh ]; then sh tests/bridge_readiness_test.sh ./$(PROGRAM); fi

clean:
	rm -f $(PROGRAM) $(CONTRACT_JSON) $(CONTRACT_HEADER)
