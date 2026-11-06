CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
TARGET := bonfyre-lowering-workers
BONFYRE_CFLAGS := -I../../lib/libbonfyre/include -I../../engine/core/include
BONFYRE_LIB := ../../lib/libbonfyre/libbonfyre.a
CORE_LIB := ../../engine/core/libbonfyre-fabric.a
.PHONY: all clean test
all: $(TARGET)
$(TARGET): src/main.c $(CORE_LIB)
	$(CC) $(CFLAGS) $(BONFYRE_CFLAGS) -o $@ $< $(BONFYRE_LIB) $(CORE_LIB) -lsqlite3
$(CORE_LIB):
	$(MAKE) -C ../../engine/core
test: $(TARGET)
	./tests/lowering_workers_test.sh ./$(TARGET)
clean:
	rm -f $(TARGET)
