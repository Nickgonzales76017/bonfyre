CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
TARGET := bonfyre-pack-condition-delta

.PHONY: all test clean native-witness deps

all: $(TARGET) deps test

$(TARGET): src/main.c
	$(CC) $(CFLAGS) -o $@ $< -lsqlite3

deps:
	$(MAKE) -C ../BonfyrePackCondition
	$(MAKE) -C ../BonfyreScopedDemand bonfyre-scoped-demand
	$(MAKE) -C ../BonfyreScopedFrontier bonfyre-scoped-frontier
	$(MAKE) -C ../BonfyreScopedReheat bonfyre-scoped-reheat

test: $(TARGET) deps
	sh tests/pack_condition_delta_test.sh ./$(TARGET) ../BonfyrePackCondition/bonfyre-pack-condition ../BonfyreScopedDemand/bonfyre-scoped-demand ../BonfyreScopedFrontier/bonfyre-scoped-frontier ../BonfyreScopedReheat/bonfyre-scoped-reheat

clean:
	rm -f $(TARGET)

native-witness: test
