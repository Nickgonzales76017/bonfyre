CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
TARGET := bonfyre-owner-contract
.PHONY: all clean test
all: $(TARGET) test
$(TARGET): src/main.c src/catalog.c src/catalog.h
	$(CC) $(CFLAGS) -o $@ src/main.c src/catalog.c
test: $(TARGET)
	./$(TARGET) classify ../../architecture/atlas.index.json --summary >/tmp/bonfyre-owner-contract-summary.json || { cat /tmp/bonfyre-owner-contract-summary.json; exit 1; }
	grep -q '"unresolved":0' /tmp/bonfyre-owner-contract-summary.json
	./$(TARGET) audit-catalog ../../architecture/atlas.index.json $$HOME/.bonfyre/bin/bonfyre-capability --summary >/tmp/bonfyre-owner-contract-catalog.json
	grep -q '"realized_unmapped":0' /tmp/bonfyre-owner-contract-catalog.json
	tmp=$$(mktemp -d); trap 'rm -rf "$$tmp"' EXIT; ./$(TARGET) image ../../architecture/atlas.index.json $$HOME/.bonfyre/bin/bonfyre-capability --out "$$tmp/system-image.json" >/tmp/bonfyre-owner-contract-image.json; ./$(TARGET) verify-image "$$tmp/system-image.json" >/tmp/bonfyre-owner-contract-image-verify.json; grep -q '"interactions":267' /tmp/bonfyre-owner-contract-image.json; grep -q '"exact_gates_still_required"' /tmp/bonfyre-owner-contract-image-verify.json
clean:
	rm -f $(TARGET)
