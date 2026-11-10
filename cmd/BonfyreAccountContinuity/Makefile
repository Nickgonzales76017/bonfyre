CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11 -D_DARWIN_C_SOURCE
CPPFLAGS += -I../../lib/libbonfyre/include
LDFLAGS += -lsqlite3

all: bonfyre-account-continuity
bonfyre-account-continuity: src/main.c ../../lib/libbonfyre/bf_msgpack.c ../../lib/libbonfyre/src/bf_sha256.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ $^ $(LDFLAGS)
