CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
CFLAGS += -I../BonfyreLex/src
LOCAL_CFLAGS = -I../../engine/core/include -I../../lib/libbonfyre/include
LDFLAGS ?= ../../engine/core/libbonfyre-fabric.a -lsqlite3
TARGET = bonfyre-admission

.PHONY: all clean test

all: $(TARGET)

$(TARGET): src/main.c ../../engine/core/libbonfyre-fabric.a ../../lib/libbonfyre/include/bf_command_lookup.h
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $@ $< $(LDFLAGS)

../../engine/core/libbonfyre-fabric.a:
	$(MAKE) -C ../../engine/core

test: $(TARGET)
	bash ./tests/smoke.sh ./$(TARGET)

clean:
	rm -f $(TARGET)
