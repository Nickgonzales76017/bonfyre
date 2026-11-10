CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
CFLAGS += -I../../engine/core/include -I../../lib/libbonfyre/include
TARGET = bonfyre-household
SRC = src/main.c ../../engine/core/src/semantic_operator.c ../../lib/libbonfyre/src/bf_sha256.c

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC)

clean:
	rm -f $(TARGET)
