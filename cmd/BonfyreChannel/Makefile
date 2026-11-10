CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
TARGET = bonfyre-channel

all: $(TARGET)

$(TARGET): src/main.c
	$(CC) $(CFLAGS) -o $@ $< -lsqlite3

test: $(TARGET)
	./tests/channel_test.sh ./$(TARGET)

.PHONY: all test
