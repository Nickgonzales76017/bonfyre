CC      ?= cc
CFLAGS  ?= -O2 -Wall -Wextra -std=c11
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LDFLAGS  = -L../../lib/libbonfyre -lbonfyre -lm
TARGET   = bonfyre-tone
SRC      = src/main.c

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

.PHONY: test
test: $(TARGET)
	$(MAKE) -C ../../tests/vectors bonfyre-vectors
	cd ../.. && tests/vectors/bonfyre-vectors --root . cmd/BonfyreTone/tests/vectors/command_behavior.json

clean:
	rm -f $(TARGET)
