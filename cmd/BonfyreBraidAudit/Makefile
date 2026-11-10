CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
TARGET := bonfyre-braid-audit
.PHONY: all clean test
all: $(TARGET) test
$(TARGET): src/main.c
	$(CC) $(CFLAGS) -o $@ src/main.c
test: $(TARGET)
	./$(TARGET) ../../architecture/atlas.index.json --runtime-evidence runtime_evidence.tsv --summary >/tmp/bonfyre-braid-summary.json
	cat /tmp/bonfyre-braid-summary.json
	grep -q '"runtime_supported_declaration_drifts":6' /tmp/bonfyre-braid-summary.json
	./$(TARGET) ../../architecture/atlas.index.json --runtime-evidence runtime_evidence.tsv >/tmp/bonfyre-braid-detail.txt
	grep -q '^PURE_SINK[[:space:]]atomic-form[[:space:]]' /tmp/bonfyre-braid-detail.txt
	grep -q 'RUNTIME_JOIN.*SemanticOperation.*runtime_closed' /tmp/bonfyre-braid-detail.txt
	grep -q 'RUNTIME_JOIN.*TransportLease.*declaration_drift' /tmp/bonfyre-braid-detail.txt
	grep -q 'RUNTIME_JOIN.*FactDelta.*distributed_runtime_projection' /tmp/bonfyre-braid-detail.txt
