CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
TARGET = bonfyre-index
SRC = src/main.c

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $(TARGET) $(SRC) ../../lib/libbonfyre/libbonfyre.a -lsqlite3

clean:
	rm -f $(TARGET)

.PHONY: all clean
