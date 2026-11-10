CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
CFLAGS += -I../../cmd/BonfyreLex/src

TARGET := bonfyre-lex
RULES := ../../schemas/control/text-rules.cue

.PHONY: all clean test native-witness unit-witness
all: $(TARGET) unit-witness native-witness

$(TARGET): src/main.c src/bf_lex.h
	$(CC) $(CFLAGS) -o $@ src/main.c -lsqlite3

# The extractors themselves, on the exact inputs that broke their copies. This
# runs before the ruleset witness because a wrong reader makes a passing
# ruleset meaningless.
unit-witness: tests/bf_lex_test.c src/bf_lex.h
	$(CC) $(CFLAGS) -o tests/bf-lex-test tests/bf_lex_test.c
	./tests/bf-lex-test

native-witness: $(TARGET)
	./tests/lex_test.sh ./$(TARGET) $(RULES)

test: unit-witness native-witness

clean:
	rm -f $(TARGET) tests/bf-lex-test
