ROOT := ../..
EGGLOG ?= $(HOME)/.bonfyre/substrates/v6.1/egglog/target/release/egglog
INPUT := generated/inverse-programs.json
PROGRAM := generated/inverse-programs.egg
NATIVE := generated/inverse_programs.h
GENSTAMP := generated/inverse-programs.stamp

.PHONY: all test verify
TARGET := bonfyre-semantic-optimizer
BIN := $(TARGET)
LIVE_TEST := live-inverse-test
ESTATE_TEST := estate-evidence-test
LIBBF := $(ROOT)/lib/libbonfyre/libbonfyre.a
all: test

$(BIN): src/main.c src/entry.c src/estate_evidence.c src/estate_evidence.h $(NATIVE) $(ROOT)/engine/core/libbonfyre-fabric.a $(LIBBF)
	$(CC) -O2 -Wall -Wextra -Werror -std=c11 -Dmain=bf_inverse_main -I$(ROOT)/engine/core/include -I$(ROOT)/lib/libbonfyre/include -Igenerated -Isrc -o $@ src/main.c src/entry.c src/estate_evidence.c $(ROOT)/engine/core/libbonfyre-fabric.a $(LIBBF) -lsqlite3

$(LIVE_TEST): tests/live_inverse_test.c $(BIN) $(ROOT)/engine/core/libbonfyre-fabric.a
	$(CC) -O2 -Wall -Wextra -Werror -std=c11 -I$(ROOT)/engine/core/include -Igenerated -o $@ tests/live_inverse_test.c $(ROOT)/engine/core/libbonfyre-fabric.a -lsqlite3

$(ROOT)/engine/core/libbonfyre-fabric.a: $(ROOT)/engine/core/src/reservation.c $(ROOT)/engine/core/src/foreign_twin.c
	$(MAKE) -C $(ROOT)/engine/core

$(INPUT): $(ROOT)/schemas/control/inverse-compiler.cue $(ROOT)/schemas/control/owner-predicate-contracts.cue
	@mkdir -p generated
	@cd $(ROOT)/schemas/control && cue export . -e inverseCompiler --out json > ../../cmd/BonfyreSemanticOptimizer/$(INPUT)

# One compiler invocation owns both generated products.  Ordinary multiple
# targets race under make -j and can leave an .egg from one algebra beside a
# header from another.
$(GENSTAMP): $(INPUT) tools/compile_inverse_program.zig
	@mkdir -p generated
	zig run tools/compile_inverse_program.zig -- $(INPUT) $(PROGRAM).tmp $(NATIVE).tmp
	mv $(PROGRAM).tmp $(PROGRAM)
	mv $(NATIVE).tmp $(NATIVE)
	touch $@

$(PROGRAM) $(NATIVE): $(GENSTAMP)

verify: $(PROGRAM)
	@test -x "$(EGGLOG)"
	$(EGGLOG) $(PROGRAM)

test: verify $(BIN) $(LIVE_TEST)
	sh tests/inverse_compiler_test.sh
	./$(LIVE_TEST) ./$(BIN)
$(ESTATE_TEST): tests/estate_evidence_test.c $(BIN) $(LIBBF)
	$(CC) -O2 -Wall -Wextra -Werror -std=c11 -I$(ROOT)/lib/libbonfyre/include -o $@ tests/estate_evidence_test.c $(LIBBF) -lsqlite3
.PHONY: estate-evidence-verify
estate-evidence-verify: $(ESTATE_TEST)
	./$(ESTATE_TEST) ./$(BIN)
verify: estate-evidence-verify
