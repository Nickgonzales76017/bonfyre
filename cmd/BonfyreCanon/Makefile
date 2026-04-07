CC       ?= cc
CFLAGS    = -O2 -Wall -Wextra -std=c11 -I/opt/homebrew/include -Isrc
LDFLAGS   = -L/opt/homebrew/lib -ltree-sitter
TARGET    = bonfyre-canon
SRC       = src/main.c src/ts_json_parser.c

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

clean:
	rm -f $(TARGET)
