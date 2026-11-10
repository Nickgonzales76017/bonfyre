CC      ?= cc
CFLAGS  ?= -O2 -Wall -Wextra -std=c11 -I../../lib/libbonfyre/include
LDFLAGS ?= ../../lib/libbonfyre/libbonfyre.a -lsqlite3 -lpthread
TARGET   = bonfyre-api

all: $(TARGET) contract-test

$(TARGET): src/main.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

contract-test: $(TARGET)
	./tests/http_contract.sh

clean:
	rm -f $(TARGET)
