CC      ?= cc
CFLAGS  ?= -O2 -Wall -Wextra -std=c11 -I../../lib/libbonfyre/include
LDFLAGS ?= ../../lib/libbonfyre/libbonfyre.a -lsqlite3
TARGET   = bonfyre-auth

$(TARGET): src/main.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

clean:
	rm -f $(TARGET)
