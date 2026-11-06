CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
TARGET := bonfyre-bitnet
BONFYRE_CFLAGS := -I../../lib/libbonfyre/include -I../../engine/core/include
BONFYRE_LIB := ../../lib/libbonfyre/libbonfyre.a
FABRIC_LIB := ../../engine/core/libbonfyre-fabric.a

.PHONY: all clean test
all: $(TARGET) test
$(TARGET): src/main.c $(FABRIC_LIB)
	$(CC) $(CFLAGS) $(BONFYRE_CFLAGS) -o $@ $< $(BONFYRE_LIB) $(FABRIC_LIB) -lsqlite3 -lcurl -lm
$(FABRIC_LIB):
	$(MAKE) -C ../../engine/core
test: $(TARGET)
	./tests/contract_test.sh ./$(TARGET)
	./tests/cpp_path_test.sh ./$(TARGET)
clean:
	rm -f $(TARGET)
