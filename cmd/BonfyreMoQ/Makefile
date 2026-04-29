CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LDFLAGS = ../../lib/libbonfyre/libbonfyre.a -lsqlite3 -lm
TARGET = bonfyre-moq
SRC = src/main.c

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

clean:
	rm -f $(TARGET)

install: $(TARGET)
	install -D $(TARGET) $(DESTDIR)$(PREFIX)/bin/$(TARGET)

.PHONY: all clean install
