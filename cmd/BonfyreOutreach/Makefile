CC ?= cc
# An include path is not a tuning preference. `make all` passes CFLAGS through
# the environment, which makes `?=` skip this whole line -- so every -I on it
# vanished and the organ could not be rebuilt from scratch by the top-level
# build, only by its own. Optimisation stays overridable; what the source needs
# to compile at all is appended.
CFLAGS ?= -O2 -Wall -Wextra -std=c11
CFLAGS += -I../../lib/libbonfyre/include
LDFLAGS ?= ../../lib/libbonfyre/libbonfyre.a -lsqlite3
TARGET = bonfyre-outreach
SRC = src/main.c

all: $(TARGET) test

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

test: $(TARGET)
	sh tests/commercial_outreach_test.sh

clean:
	rm -f $(TARGET)

.PHONY: all test clean
