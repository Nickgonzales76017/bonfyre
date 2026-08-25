CC ?= cc
TARGET = bonfyre-fs

# Resolve FUSE through pkg-config where it exists. The previous hard-coded
# /usr/local paths are macFUSE's layout and silently fail to build anywhere
# else, which is why this never ran in CI.
FUSE_CFLAGS := $(shell pkg-config --cflags fuse 2>/dev/null)
FUSE_LIBS := $(shell pkg-config --libs fuse 2>/dev/null)

ifeq ($(strip $(FUSE_LIBS)),)
FUSE_CFLAGS := -I/usr/local/include/fuse
FUSE_LIBS := -L/usr/local/lib -lfuse
endif

CFLAGS ?= -O2 -Wall -Wextra -std=c11
CFLAGS += $(FUSE_CFLAGS) -D_FILE_OFFSET_BITS=64
LDFLAGS ?=
LDLIBS = $(FUSE_LIBS) -lsqlite3 -pthread

$(TARGET): src/main.c
	$(CC) $(CFLAGS) -o $(TARGET) src/main.c $(LDFLAGS) $(LDLIBS)

clean:
	rm -f $(TARGET)

.PHONY: clean
