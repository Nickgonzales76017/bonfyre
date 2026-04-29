CC      ?= cc
CFLAGS  ?= -O2 -Wall -Wextra -std=c11
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LDFLAGS_BF   = -L../../lib/libbonfyre -lbonfyre
LDFLAGS ?= $(LDFLAGS_BF) -lsqlite3
TARGET   = bonfyre-tel
SRC      = src/main.c

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

clean:
	rm -f $(TARGET)

.PHONY: all clean
