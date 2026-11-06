CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
CFLAGS += -I../../engine/core/include
TARGET = bonfyre-agent-contract
SRC = src/main.c ../../engine/core/src/agent_contract.c

.PHONY: all clean test

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC)

clean:
	rm -f $(TARGET)

test: $(TARGET)
	sh ./tests/receipt_frontier_test.sh ./$(TARGET)
