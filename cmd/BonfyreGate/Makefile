CC ?= cc
# An include path is not a tuning preference. `make all` passes CFLAGS through
# the environment, which makes `?=` skip this whole line -- so every -I on it
# vanished and the organ could not be rebuilt from scratch by the top-level
# build, only by its own. Optimisation stays overridable; what the source needs
# to compile at all is appended.
CFLAGS ?= -O2 -Wall -Wextra -std=c11
CFLAGS += -I../../lib/libbonfyre/include
CFLAGS += -I../../cmd/BonfyreLex/src
LDFLAGS ?= ../../lib/libbonfyre/libbonfyre.a -lsqlite3
TARGET = bonfyre-gate
SRC = src/main.c

all: $(TARGET) guard-witness

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

clean:
	rm -f $(TARGET)

.PHONY: guard-witness all clean

# A license gate with no test is a license gate that fails both ways in silence.
guard-witness: $(TARGET)
	$(MAKE) -C ../BonfyreSign
	./tests/guard_test.sh ./$(TARGET)
