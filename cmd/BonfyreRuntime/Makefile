CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LDFLAGS = -L../../lib/libbonfyre -lbonfyre -lsqlite3
TARGET = bonfyre-runtime

all: $(TARGET)

$(TARGET): src/main.c ../../lib/libbonfyre/include/bf_command_lookup.h
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $@ src/main.c $(LDFLAGS)

clean:
	rm -f $(TARGET)
