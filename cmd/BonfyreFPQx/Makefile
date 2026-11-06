CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LDFLAGS = -L../../lib/libbonfyre -lbonfyre -lm
TARGET = bonfyre-fpqx
SRC = src/main.c

all: $(TARGET) test

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

test: $(TARGET)
	sh tests/bqfp_contract_test.sh ./$(TARGET)

clean:
	rm -f $(TARGET)

.PHONY: all clean test
