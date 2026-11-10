CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
CFLAGS += -I../../engine/core/include
TARGET = bonfyre-retention
SRC = src/main.c ../../engine/core/src/retention.c

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC) -lsqlite3

clean:
	rm -f $(TARGET)
