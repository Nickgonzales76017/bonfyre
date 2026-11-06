CC ?= cc
CFLAGS ?= -Os -Wall -Wextra -std=c11
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
# Unreferenced library code stays out of the installed program.
ifeq ($(shell uname -s),Darwin)
DEAD_STRIP = -Wl,-dead_strip
else
DEAD_STRIP = -Wl,--gc-sections
endif
LDFLAGS  = ../../lib/libbonfyre/libbonfyre.a -lm $(DEAD_STRIP)
TARGET = bonfyre-kvcache
SRC = src/main.c
LIB = ../../lib/libbonfyre/libbonfyre.a

all: $(TARGET) contract-test

$(TARGET): $(SRC) $(LIB) ../../lib/libbonfyre/include/bonfyre.h
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -MMD -MP -MF $(TARGET).d -o $(TARGET) $(SRC) $(LDFLAGS)

$(LIB):
	$(MAKE) -C ../../lib/libbonfyre libbonfyre.a

publication-test: $(TARGET)
	node --test tests/context_publication.test.mjs

contract-test: publication-test
	./tests/context_contract.sh

-include $(TARGET).d

clean:
	rm -f $(TARGET) $(TARGET).d

.PHONY: all clean publication-test contract-test
