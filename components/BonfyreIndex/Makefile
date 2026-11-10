CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
CFLAGS += -I../../cmd/BonfyreLex/src
TARGET = bonfyre-index
SRC = src/main.c

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC) -lsqlite3

clean:
	rm -f $(TARGET)

.PHONY: all clean
