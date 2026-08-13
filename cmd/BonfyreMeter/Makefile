CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11 -I../../lib/libbonfyre/include
TARGET = bonfyre-meter
SRC = src/main.c

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC) ../../lib/libbonfyre/libbonfyre.a -lsqlite3

clean:
	rm -f $(TARGET)

.PHONY: all clean
