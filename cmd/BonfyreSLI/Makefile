CC      ?= cc
CFLAGS  ?= -O2 -Wall -Wextra -std=c11
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LDFLAGS      = -L../../lib/libbonfyre -lbonfyre -lm
TARGET       = bonfyre-sli
SRC          = src/main.c

.PHONY: all clean test

all: $(TARGET) test

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $@ $^ $(LDFLAGS)

test: $(TARGET)
	sh tests/bqfp_corruption_test.sh ./$(TARGET)

clean:
	rm -f $(TARGET)
