CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
TARGET := bonfyre-websub-lease-gate

.PHONY: all test clean native-witness FORCE

all: $(TARGET) test

$(TARGET): FORCE src/main.c
	$(CC) $(CFLAGS) -o $@ src/main.c -lsqlite3

test: $(TARGET)
	sh tests/lease_gate_test.sh ./$(TARGET)

clean:
	rm -f $(TARGET)

native-witness: test
