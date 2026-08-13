CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11 -I../../lib/libbonfyre/include
TARGET = bonfyre-hash
SRC = src/main.c

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC) ../../lib/libbonfyre/libbonfyre.a

clean:
	rm -f $(TARGET)

.PHONY: all clean
