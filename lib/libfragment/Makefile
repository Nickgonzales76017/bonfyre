CC      ?= cc
OPENSSL_CFLAGS := $(shell pkg-config --cflags openssl  2>/dev/null || echo "-I/opt/homebrew/opt/openssl@3/include")
OPENSSL_LIBS   := $(shell pkg-config --libs   openssl  2>/dev/null || echo "-L/opt/homebrew/opt/openssl@3/lib -lssl -lcrypto")
SQLITE_CFLAGS  := $(shell pkg-config --cflags sqlite3  2>/dev/null || echo "-I/opt/homebrew/opt/sqlite/include")
SQLITE_LIBS    := $(shell pkg-config --libs   sqlite3  2>/dev/null || echo "-L/opt/homebrew/opt/sqlite/lib -lsqlite3")
CFLAGS  := -O2 -Wall -Wextra -std=c11 \
           -I./include \
           $(OPENSSL_CFLAGS) $(SQLITE_CFLAGS)
LDFLAGS := $(OPENSSL_LIBS) \
           $(SQLITE_LIBS)

SRC     := fragment.c
OBJ_DIR := obj
LIB     := libfragment.a

.PHONY: all clean test

all: $(LIB)

$(LIB): $(OBJ_DIR)/fragment.o
	ar rcs $@ $^

$(OBJ_DIR)/fragment.o: $(SRC) include/fragment.h | $(OBJ_DIR)
	$(CC) $(CFLAGS) -c -o $@ $(SRC)

$(OBJ_DIR):
	mkdir -p $(OBJ_DIR)

clean:
	rm -rf $(OBJ_DIR) $(LIB)

test: $(LIB)
	$(CC) $(CFLAGS) -o obj/test_fragment test/test_fragment.c \
	    -L. -lfragment $(LDFLAGS)
	./obj/test_fragment
