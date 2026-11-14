CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LDFLAGS  = -L../../lib/libbonfyre -lbonfyre -lm
TARGET = bonfyre-reason
SRC = src/main.c

all: $(TARGET) offline-test

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

offline-test: $(TARGET)
	sh tests/session_offline.sh

# Live endpoint model test remains explicitly opt-in.

contract-test: $(TARGET)
	./tests/session_contract.sh

test: offline-test contract-test

clean:
	rm -f $(TARGET)

.PHONY: all clean offline-test test contract-test
