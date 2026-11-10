CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
LOCAL_CFLAGS = -I../../lib/libbonfyre/include -D_DARWIN_C_SOURCE -D_DEFAULT_SOURCE
LDFLAGS = -L../../lib/libbonfyre -lbonfyre -lsqlite3 -lm
TARGET = bonfyre-bridge
SRC = src/main.c src/transform.c

.PHONY: all clean test

all: $(TARGET)

$(TARGET): $(SRC) tests/column_readback_test.sh
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $@ $(SRC) $(LDFLAGS)
	sh tests/column_readback_test.sh ./$(TARGET)

test: $(TARGET)
	./tests/db_bridge_test.sh ./$(TARGET)
	sh tests/transform_evidence_test.sh ./$(TARGET)

MANAGED_PREFIX ?= $(HOME)/.bonfyre
install: all
	install -d "$(MANAGED_PREFIX)/bin"
	install -m 755 $(TARGET) "$(MANAGED_PREFIX)/bin/.bonfyre-bridge.next"
	mv -f "$(MANAGED_PREFIX)/bin/.bonfyre-bridge.next" "$(MANAGED_PREFIX)/bin/bonfyre-bridge"

clean:
	rm -f $(TARGET)

.PHONY: all clean test install
