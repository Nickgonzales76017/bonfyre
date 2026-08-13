CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11 -I/usr/local/include/fuse -D_FILE_OFFSET_BITS=64
LDFLAGS ?= -L/usr/local/lib -lfuse -lsqlite3 -pthread
TARGET = bonfyre-fs

$(TARGET): src/main.c
	$(CC) $(CFLAGS) -o $(TARGET) src/main.c $(LDFLAGS)

clean:
	rm -f $(TARGET)

.PHONY: clean
