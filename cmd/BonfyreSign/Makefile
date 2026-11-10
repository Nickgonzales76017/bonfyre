CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
SODIUM_CFLAGS := $(shell pkg-config --cflags libsodium 2>/dev/null)
SODIUM_LIBS := $(shell pkg-config --libs libsodium 2>/dev/null || echo -lsodium)
TARGET := bonfyre-sign

.PHONY: all clean test
all: $(TARGET) test
$(TARGET): src/main.c
	$(CC) $(CFLAGS) $(SODIUM_CFLAGS) -o $@ $< $(SODIUM_LIBS)
test: $(TARGET)
	./tests/sign_test.sh ./$(TARGET)
clean:
	rm -f $(TARGET)
