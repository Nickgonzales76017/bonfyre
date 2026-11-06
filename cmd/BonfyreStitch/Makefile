CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11 -ffunction-sections -fdata-sections
CFLAGS += -I../../cmd/BonfyreLex/src
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LDFLAGS  = ../../lib/libbonfyre/libbonfyre.a -lsqlite3
TARGET = bonfyre-stitch
SRC = src/main.c

all: $(TARGET)

test: $(TARGET)
	sh ./tests/dispatch_test.sh ./$(TARGET)

$(TARGET): $(SRC) ../../lib/libbonfyre/include/bf_command_lookup.h ../../lib/libbonfyre/libbonfyre.a
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -Wl,-dead_strip -o $(TARGET) $(SRC) $(LDFLAGS)

clean:
	rm -f $(TARGET)

.PHONY: all clean test
