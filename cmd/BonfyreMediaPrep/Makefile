CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LDFLAGS  = -L../../lib/libbonfyre -lbonfyre
TARGET = bonfyre-media-prep
SRC = src/main.c

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

test: $(TARGET)
	cd ../.. && tests/vectors/bonfyre-vectors --root . cmd/BonfyreMediaPrep/tests/vectors/command_behavior.json

MANAGED_PREFIX ?= $(HOME)/.bonfyre
install: all
	install -d "$(MANAGED_PREFIX)/bin"
	install -m 755 $(TARGET) "$(MANAGED_PREFIX)/bin/.bonfyre-media-prep.next"
	mv -f "$(MANAGED_PREFIX)/bin/.bonfyre-media-prep.next" "$(MANAGED_PREFIX)/bin/bonfyre-media-prep"

clean:
	rm -f $(TARGET)

.PHONY: all clean test install
