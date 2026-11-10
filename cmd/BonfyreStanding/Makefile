CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
CFLAGS += -I../../engine/core/include
TARGET = bonfyre-standing
SRC = src/main.c ../../engine/core/src/standing_address.c ../../engine/core/src/information_policy.c

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC) -lsqlite3

clean:
	rm -f $(TARGET)
