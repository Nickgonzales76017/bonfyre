CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11 -I/opt/homebrew/include/onnxruntime
LDFLAGS ?= -L/opt/homebrew/lib -lonnxruntime -lm
TARGET = bonfyre-embed
SRC = src/main.c

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

clean:
	rm -f $(TARGET)

.PHONY: all clean
