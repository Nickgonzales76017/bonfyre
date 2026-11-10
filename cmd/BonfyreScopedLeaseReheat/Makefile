CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
TARGET := bonfyre-scoped-lease-reheat

.PHONY: all test clean native-witness deps FORCE

all: $(TARGET) deps test

$(TARGET): FORCE src/main.c
	$(CC) $(CFLAGS) -o $@ src/main.c -lsqlite3

deps:
	$(MAKE) -C ../BonfyreScopedDemand bonfyre-scoped-demand
	$(MAKE) -C ../BonfyreScopedFrontier bonfyre-scoped-frontier
	$(MAKE) -C ../BonfyreMaterialLease bonfyre-material-lease

test: $(TARGET) deps
	sh tests/scoped_lease_reheat_test.sh ./$(TARGET) ../BonfyreScopedDemand/bonfyre-scoped-demand ../BonfyreScopedFrontier/bonfyre-scoped-frontier ../BonfyreMaterialLease/bonfyre-material-lease

clean:
	rm -f $(TARGET)

native-witness: test
FORCE:
