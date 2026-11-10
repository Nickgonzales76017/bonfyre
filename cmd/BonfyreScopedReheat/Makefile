CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
TARGET := bonfyre-scoped-reheat

.PHONY: all test clean native-witness deps

all: $(TARGET) deps test

$(TARGET): src/main.c
	$(CC) $(CFLAGS) -o $@ $< -lsqlite3

deps:
	$(MAKE) -C ../BonfyreScopedDemand bonfyre-scoped-demand
	$(MAKE) -C ../BonfyreScopedFrontier bonfyre-scoped-frontier

test: $(TARGET) deps
	sh tests/scoped_reheat_test.sh ./$(TARGET) ../BonfyreScopedDemand/bonfyre-scoped-demand ../BonfyreScopedFrontier/bonfyre-scoped-frontier

clean:
	rm -f $(TARGET)

native-witness: test
