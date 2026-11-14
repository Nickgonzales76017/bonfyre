CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
PROGRAM := bonfyre-participation
CONTRACT_JSON := generated/participation-contract.json
CONTRACT_HEADER := generated/participation_contract.h
ACTIVATION_JSON := generated/activation-contract.json
ACTIVATION_HEADER := generated/activation_contract.h
SRC := src/entry.c src/json.c src/evidence.c src/projection.c src/activation.c src/dating.c

.PHONY: all clean test

all: test

$(CONTRACT_JSON): participation-contract.cue
	mkdir -p generated
	cue export participation-contract.cue -e participationContract --out json > $@

$(CONTRACT_HEADER): $(CONTRACT_JSON) tools/compile_participation_contract.zig
	zig run tools/compile_participation_contract.zig -- $(CONTRACT_JSON) $(CONTRACT_HEADER)

$(ACTIVATION_JSON): activation-contract.cue
	mkdir -p generated
	cue export activation-contract.cue -e activationContract --out json > $@

$(ACTIVATION_HEADER): $(ACTIVATION_JSON) tools/compile_activation_contract.zig
	zig run tools/compile_activation_contract.zig -- $(ACTIVATION_JSON) $(ACTIVATION_HEADER)

$(PROGRAM): $(SRC) $(CONTRACT_HEADER) $(ACTIVATION_HEADER)
	$(CC) $(CFLAGS) -Igenerated -Isrc -o $@ $(SRC) -lsqlite3

test: $(PROGRAM) dating-owner-witness dating-init-witness
	sh tests/participation_compat_test.sh ./$(PROGRAM)
	sh tests/activation_compat_test.sh ./$(PROGRAM)

dating-owner-witness:
	sh ../BonfyreWebAddress/tests/dating_policy_test.sh
	@set -eu; bin=$$(mktemp /tmp/bonfyre-dating-owner.XXXXXX); \
	trap 'rm -f "$$bin"' 0 1 2 15; $(CC) $(CFLAGS) -Isrc -o "$$bin" tests/dating_owner_test.c src/dating.c -lsqlite3; "$$bin"

.PHONY: dating-init-witness
dating-init-witness: $(PROGRAM)
	sh tests/dating_store_init_test.sh ./$(PROGRAM)
clean:
	rm -f $(PROGRAM) $(CONTRACT_JSON) $(CONTRACT_HEADER) $(ACTIVATION_JSON) $(ACTIVATION_HEADER)
