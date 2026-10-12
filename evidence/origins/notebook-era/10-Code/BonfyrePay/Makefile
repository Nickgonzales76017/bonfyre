CC      ?= cc
CFLAGS  ?= -O2 -Wall -Wextra -std=c11
LDFLAGS ?= -lsqlite3
TARGET   = bonfyre-pay

$(TARGET): src/main.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

clean:
	rm -f $(TARGET)
