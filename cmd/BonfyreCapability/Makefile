CC      = cc
CFLAGS  = -std=c11 -O2 -Wall -Wextra -Wpedantic
LOCAL_CFLAGS = -I../../cmd/BonfyreLex/src -I../../lib/libbonfyre/include -I../../engine/core/include
LDFLAGS = -L../../engine/core -lbonfyre-fabric -L../../lib/libbonfyre -lbonfyre -lsqlite3
TARGET  = bonfyre-capability
SRC     = src/main.c

.PHONY: all clean witnesses discovery-witness owner-contract-realization-witness
all: $(TARGET) witnesses

witnesses: $(TARGET)
	./tests/discovery_test.sh ./$(TARGET)
	./tests/owner_contract_realization_test.sh ./$(TARGET)

$(TARGET): $(SRC) ../../engine/core/libbonfyre-fabric.a
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $@ $(SRC) $(LDFLAGS)

../../engine/core/libbonfyre-fabric.a:
	$(MAKE) -C ../../engine/core

clean:
	rm -f $(TARGET)

# A command named index must discover, or be named something else.
discovery-witness: $(TARGET)
	./tests/discovery_test.sh ./$(TARGET)

# A registry entry is an executable advertisement only when its binary and
# invocation contract name a current native owner.  This uses a throwaway DB;
# it never mutates a caller's capability catalog.
owner-contract-realization-witness: $(TARGET)
	./tests/owner_contract_realization_test.sh ./$(TARGET)
