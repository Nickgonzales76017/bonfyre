CC ?= cc
# An include path is not a tuning preference. `make all` passes CFLAGS through
# the environment, which makes `?=` skip this whole line -- so every -I on it
# vanished and the organ could not be rebuilt from scratch by the top-level
# build, only by its own. Optimisation stays overridable; what the source needs
# to compile at all is appended.
CFLAGS ?= -O2 -Wall -Wextra -std=c11
CFLAGS += -I../../lib/libbonfyre/include
TARGET = bonfyre-hash
SRC = src/main.c src/sqlite_cut.c
LIB = ../../lib/libbonfyre/libbonfyre.a

all: $(TARGET)

$(LIB): force-library
	$(MAKE) -C ../../lib/libbonfyre

$(TARGET): $(SRC) src/sqlite_cut.h $(LIB)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC) $(LIB) -lsqlite3

test: $(TARGET)
	bun test tests/file.test.mjs tests/sqlite-cut.test.mjs

clean:
	rm -f $(TARGET)

.PHONY: all clean test force-library
