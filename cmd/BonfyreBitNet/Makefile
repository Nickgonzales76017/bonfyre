CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
TARGET := bonfyre-bitnet
BONFYRE_CFLAGS := -I../../lib/libbonfyre/include -I../../engine/core/include
BONFYRE_LIB := ../../lib/libbonfyre/libbonfyre.a
FABRIC_LIB := ../../engine/core/libbonfyre-fabric.a

.PHONY: all clean test
all: $(TARGET) test
$(TARGET): src/main.c generated/store_schema.h $(FABRIC_LIB)
	$(CC) $(CFLAGS) $(BONFYRE_CFLAGS) -o $@ $< $(BONFYRE_LIB) $(FABRIC_LIB) -lsqlite3 -lcurl -lm
$(FABRIC_LIB):
	$(MAKE) -C ../../engine/core
test: $(TARGET)
	./tests/contract_test.sh ./$(TARGET)
	./tests/cpp_path_test.sh ./$(TARGET)
	$(MAKE) ranker-store-kind-test

# The ranker store kind (schemas/storage/stores, store rank) builds and reads back.
.PHONY: ranker-store-kind-test
ranker-store-kind-test: tests/ranker_store_kind_test.c generated/store_schema.h
	$(CC) $(CFLAGS) $(BONFYRE_CFLAGS) -o tests/.ranker-store-kind tests/ranker_store_kind_test.c -lsqlite3
	./tests/.ranker-store-kind; status=$$?; rm -f tests/.ranker-store-kind; exit $$status
clean:
	rm -f $(TARGET)
