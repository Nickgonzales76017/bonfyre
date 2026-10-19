CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11 -I../../lib/libbonfyre/include
LDLIBS ?= -lm
TARGET = bonfyre-scene-detect
SRC = src/main.c

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC) ../../lib/libbonfyre/libbonfyre.a $(LDLIBS)

clean:
	rm -f $(TARGET)

.PHONY: all clean
