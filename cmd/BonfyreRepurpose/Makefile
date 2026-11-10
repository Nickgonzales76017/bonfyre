CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LDFLAGS  = -L../../lib/libbonfyre -lbonfyre
TARGET = bonfyre-repurpose
SRC = src/main.c

all: $(TARGET)

test: $(TARGET)
	$(MAKE) -C ../BonfyreBrief
	sh tests/transcript_handoff_test.sh

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

clean:
	rm -f $(TARGET)

.PHONY: all test clean
