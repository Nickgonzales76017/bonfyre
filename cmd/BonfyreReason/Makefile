CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LDFLAGS  = -L../../lib/libbonfyre -lbonfyre -lm
TARGET = bonfyre-reason
SRC = src/main.c

all: $(TARGET) contract-test

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

contract-test: $(TARGET)
	./tests/session_contract.sh

clean:
	rm -f $(TARGET)

.PHONY: all clean
