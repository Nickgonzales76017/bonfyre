CC      ?= cc
CFLAGS   = -O2 -Wall -Wextra -std=c11
TARGET   = bonfyre-query
SRC      = src/main.c

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC)

clean:
	rm -f $(TARGET)
