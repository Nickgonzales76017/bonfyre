CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
CFLAGS += -I../BonfyreLex/src
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
TARGET = bonfyre-index
SRC = src/main.c

all: $(TARGET)

$(TARGET): $(SRC) ../../lib/libbonfyre/include/bf_command_lookup.h
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $(TARGET) $(SRC) ../../lib/libbonfyre/libbonfyre.a -lsqlite3

clean:
	rm -f $(TARGET)

.PHONY: all clean
