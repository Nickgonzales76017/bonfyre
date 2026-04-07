CC      ?= cc
CFLAGS   = -O2 -Wall -Wextra -std=c11
TARGET   = bonfyre-tone
SRC      = src/main.c

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC) -lm

clean:
	rm -f $(TARGET)
