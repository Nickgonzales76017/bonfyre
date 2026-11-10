CC     ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
CFLAGS += -I../BonfyreLex/src
LOCAL_CFLAGS = -I../../lib/libbonfyre/include -I../../engine/core/include
# Dead stripping keeps the installed organ inside the native ratchet's byte budget.
comma := ,
LDFLAGS  = -lsqlite3 $(if $(filter Darwin,$(shell uname -s)),-Wl$(comma)-dead_strip,-Wl$(comma)--gc-sections)
LIB = ../../lib/libbonfyre/libbonfyre.a
# The memory ledger every model loader asks before it loads.
FABRIC = ../../engine/core/libbonfyre-fabric.a
TARGET  = bonfyre-model
SRC     = src/main.c

all: $(TARGET)

test: $(TARGET)
	python3 ./tests/manifest_test.py ./$(TARGET)
	sh ./tests/manifest_test.sh ./$(TARGET)
	./tests/representation_passport_test.sh ./$(TARGET)
	python3 ./tests/file_verification_test.py ./$(TARGET)
	sh ./tests/file_verification_test.sh ./$(TARGET)
	./tests/local_test.sh ./$(TARGET)

$(LIB): force-library
	$(MAKE) -C ../../lib/libbonfyre

$(TARGET): $(SRC) $(LIB)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $(TARGET) $(SRC) $(LIB) $(FABRIC) $(LDFLAGS)

MANAGED_PREFIX ?= $(HOME)/.bonfyre
install: all
	install -d "$(MANAGED_PREFIX)/bin"
	install -m 755 $(TARGET) "$(MANAGED_PREFIX)/bin/.bonfyre-model.next"
	mv -f "$(MANAGED_PREFIX)/bin/.bonfyre-model.next" "$(MANAGED_PREFIX)/bin/bonfyre-model"

clean:
	rm -f $(TARGET)

.PHONY: all clean test install force-library
