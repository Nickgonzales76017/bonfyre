CC      ?= cc
CFLAGS   = -O2 -Wall -Wextra -std=c11 -I/opt/homebrew/opt/sqlite/include
LDFLAGS  = -L/opt/homebrew/opt/sqlite/lib -lsqlite3
TARGET   = bonfyre-vec
SRC      = src/main.c

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

clean:
	rm -f $(TARGET)
