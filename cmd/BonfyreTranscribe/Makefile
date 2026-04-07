CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
LOCAL_CFLAGS = -I../../lib/libbonfyre/include -I/opt/homebrew/include
LDFLAGS  = -L../../lib/libbonfyre -lbonfyre -L/opt/homebrew/lib -lwhisper -lm
TARGET = bonfyre-transcribe
SRC = src/main.c

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

clean:
	rm -f $(TARGET)

.PHONY: all clean
