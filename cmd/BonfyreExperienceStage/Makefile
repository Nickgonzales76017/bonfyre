CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
ROOT := ../..
LIB := $(ROOT)/lib/libbonfyre
PROGRAM := bonfyre-experience-stage
VOCAB_JSON := generated/stage-vocabulary.json
VOCAB_HEADER := generated/stage_vocabulary.h
SRC := src/main.c src/chains.c src/match.c src/json_ascii.c src/compiler.c src/portfolio.c \
       $(LIB)/src/bf_sha256.c

.PHONY: all clean test

all: test

$(VOCAB_JSON): stage-vocabulary.cue
	mkdir -p generated
	cue export stage-vocabulary.cue -e stageVocabulary --out json > $@

$(VOCAB_HEADER): $(VOCAB_JSON) tools/compile_stage_vocabulary.zig
	zig run tools/compile_stage_vocabulary.zig -- $(VOCAB_JSON) $(VOCAB_HEADER)

$(PROGRAM): $(SRC) src/portfolio.h experience-maturities.yaff $(VOCAB_HEADER)
	$(CC) $(CFLAGS) -Igenerated -I$(LIB)/include -o $@ $(SRC) -lsqlite3

test: $(PROGRAM)
	sh tests/stage_compiler_test.sh ./$(PROGRAM)
	sh tests/portfolio_test.sh ./$(PROGRAM)
	sh tests/portfolio_queue_test.sh ./$(PROGRAM)

clean:
	rm -f $(PROGRAM) $(VOCAB_JSON) $(VOCAB_HEADER)
