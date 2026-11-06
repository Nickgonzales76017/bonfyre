CC ?= cc
# An include path is not a tuning preference. `make all` passes CFLAGS through
# the environment, which makes `?=` skip this whole line -- so every -I on it
# vanished and the organ could not be rebuilt from scratch by the top-level
# build, only by its own. Optimisation stays overridable; what the source needs
# to compile at all is appended.
CFLAGS ?= -O2 -Wall -Wextra -std=c11
CFLAGS += -I../../lib/libbonfyre/include
TARGET = bonfyre-scene-detect
SRC = src/main.c

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC) ../../lib/libbonfyre/libbonfyre.a -lm
# -lm: llround and friends live in libm on glibc. macOS folds libm into
# libSystem, so this link succeeded there and failed only on the deploy host.

clean:
	rm -f $(TARGET)

.PHONY: all clean
