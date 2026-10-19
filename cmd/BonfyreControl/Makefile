CC      ?= cc
CFLAGS  = -std=c11 -O2 -Wall -Wextra -Wpedantic
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LDFLAGS = -L../../lib/libbonfyre -lbonfyre -lsqlite3 -lm
TARGET  = bonfyre-control
SRC     = src/main.c

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $@ $(SRC) $(LDFLAGS)

clean:
	rm -f $(TARGET)
