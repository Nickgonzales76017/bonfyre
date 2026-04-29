CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
LDFLAGS ?= ../../lib/libbonfyre/libbonfyre.a -lsqlite3
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
TARGET = bonfyre-graph
SRC = src/main.c

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

clean:
	rm -f $(TARGET)

.PHONY: all clean
