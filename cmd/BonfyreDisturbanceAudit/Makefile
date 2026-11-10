CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
PROGRAM := bonfyre-disturbance-audit
CONTRACT_JSON := generated/contract.json
CONTRACT_HEADER := generated/disturbance_contract.h

.PHONY: all clean test

all: test

$(CONTRACT_JSON): contract.cue
	mkdir -p generated
	cue export contract.cue -e disturbanceContract --out json > $@

$(CONTRACT_HEADER): $(CONTRACT_JSON) tools/compile_contract.zig
	zig run tools/compile_contract.zig -- $(CONTRACT_JSON) $(CONTRACT_HEADER)

$(PROGRAM): src/main.c $(CONTRACT_HEADER)
	$(CC) $(CFLAGS) -Igenerated -o $@ src/main.c -lsqlite3

test: $(PROGRAM)
	@if [ -f tests/disturbance_audit_test.sh ]; then sh tests/disturbance_audit_test.sh ./$(PROGRAM); fi

clean:
	rm -f $(PROGRAM) $(CONTRACT_JSON) $(CONTRACT_HEADER)
