CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
ROOT := ../..
TARGET = bonfyre-provider-estate

all: $(TARGET)

$(TARGET): src/main.c src/customer.h $(ROOT)/lib/libbonfyre/bf_jsonw.c $(ROOT)/lib/libbonfyre/bf_json.c $(ROOT)/lib/libbonfyre/src/bf_sha256.c
	$(CC) $(CFLAGS) -I$(ROOT)/lib/libbonfyre/include -o $@ src/main.c $(ROOT)/lib/libbonfyre/bf_jsonw.c $(ROOT)/lib/libbonfyre/bf_json.c $(ROOT)/lib/libbonfyre/src/bf_sha256.c -lsqlite3

test: $(TARGET)
	sh tests/provider_estate_test.sh ./$(TARGET)
	sh tests/customer_test.sh ./$(TARGET)
	./$(TARGET) check

clean:
	rm -f $(TARGET)

.PHONY: all test clean
