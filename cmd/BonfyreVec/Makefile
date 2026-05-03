CC      ?= cc
CFLAGS  ?= -O2 -Wall -Wextra -std=c11
SQLITE_CFLAGS := $(shell pkg-config --cflags sqlite3 2>/dev/null || echo "-I/opt/homebrew/opt/sqlite/include")
SQLITE_LIBS   := $(shell pkg-config --libs   sqlite3 2>/dev/null || echo "-L/opt/homebrew/opt/sqlite/lib -lsqlite3")
LOCAL_CFLAGS = $(SQLITE_CFLAGS)
LDFLAGS  = $(SQLITE_LIBS)
TARGET   = bonfyre-vec
SRC      = src/main.c

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

clean:
	rm -f $(TARGET)
