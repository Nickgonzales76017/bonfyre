CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
# gcc-only: -Wformat-truncation is heuristic on snprintf into fixed buffers.
# Probed rather than name-matched (Ubuntu's cc reports "cc", not "gcc").
CFLAGS += $(shell $(CC) -Wno-format-truncation -E -x c - </dev/null >/dev/null 2>&1 && echo -Wno-format-truncation)

TARGET = bonfyre-domain

.PHONY: all clean test
all: $(TARGET)

$(TARGET): src/main.c
	$(CC) $(CFLAGS) -o $@ $< -lcurl

test: $(TARGET)
	./tests/domain_control_test.sh

clean:
	rm -f $(TARGET)
