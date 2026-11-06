CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
ROOT := ../..
TARGET = bonfyre-credentials
OPENSSL_PREFIX ?= $(shell brew --prefix openssl@3 2>/dev/null)
ifneq ($(OPENSSL_PREFIX),)
OPENSSL_CFLAGS = -I$(OPENSSL_PREFIX)/include
OPENSSL_LIBS = -L$(OPENSSL_PREFIX)/lib
endif

all: $(TARGET)

$(TARGET): src/main.c $(ROOT)/lib/libbonfyre/bf_json.c
	$(CC) $(CFLAGS) -I$(ROOT)/lib/libbonfyre/include $(OPENSSL_CFLAGS) -o $@ src/main.c $(ROOT)/lib/libbonfyre/bf_json.c $(OPENSSL_LIBS) -lcrypto -lsqlite3

test: $(TARGET)
	./tests/credentials_test.sh ./$(TARGET)
	sh tests/accounts_test.sh ./$(TARGET)
	bun test tests/discover_credential_needs_test.mjs

clean:
	rm -f $(TARGET)

.PHONY: all test clean
