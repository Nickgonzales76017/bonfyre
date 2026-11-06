CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
TARGET := bonfyre-scoped-frontier

.PHONY: all test clean native-witness

all: $(TARGET) test

$(TARGET): src/main.c
	$(CC) $(CFLAGS) -o $@ $< -lsqlite3

test: $(TARGET)
	sh tests/scoped_frontier_test.sh ./$(TARGET)

clean:
	rm -f $(TARGET)

native-witness: test
