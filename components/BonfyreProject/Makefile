CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11 -ffunction-sections -fdata-sections
CFLAGS += -I../../cmd/BonfyreLex/src -I../../cmd/BonfyreWebAddress/generated
JSON_CFLAGS = -I../../lib/libbonfyre/include -I../../engine/core/include -I../../cmd/BonfyreLex/src -I../../cmd/BonfyreWebAddress/generated
JSON_SRC = ../../lib/libbonfyre/bf_json.c ../../lib/libbonfyre/src/bf_sha256.c ../../engine/core/src/matrix.c ../../engine/core/src/authority.c ../../engine/core/src/service_lifecycle.c ../../engine/core/src/activation.c

# The receipt pack needs zstd. Where the library is present it is linked
# statically, so the binary carries no new runtime dependency; where it is
# not, the binary is built without the pack and says so when asked for one.
ZSTD_PREFIX ?= $(shell for p in /opt/homebrew /usr/local /opt/local /usr; do [ -f $$p/include/zdict.h ] && echo $$p && break; done)
ifneq ($(ZSTD_PREFIX),)
ZSTD_CFLAGS = -DBONFYRE_HAVE_ZSTD -I$(ZSTD_PREFIX)/include
ZSTD_LIBS = $(if $(wildcard $(ZSTD_PREFIX)/lib/libzstd.a),$(ZSTD_PREFIX)/lib/libzstd.a,-L$(ZSTD_PREFIX)/lib -lzstd)
endif

all: bonfyre-project

# The generated lifecycle is a prerequisite, not a coincidence. A changed
# contract that leaves the binary alone is a contract that gates nothing.
bonfyre-project: ../../cmd/BonfyreWebAddress/generated/native_destination_release.h \
                 ../../cmd/BonfyreWebAddress/generated/native_discovery_sets.h \
                 ../../cmd/BonfyreWebAddress/generated/native_missing_cells.h

bonfyre-project: src/main.c src/message_review.h ../../cmd/BonfyreLex/src/bf_lex.h $(JSON_SRC)
	$(CC) $(CFLAGS) $(JSON_CFLAGS) $(ZSTD_CFLAGS) -Wl,-dead_strip -o $@ src/main.c $(JSON_SRC) -lsqlite3 $(ZSTD_LIBS)

# Receipts as a typed table, for questions over the whole set. Needs a
# packaged Arrow C++ SDK, simdjson and zstd; it is not part of `all`.
bonfyre-receipt-table: tools/receipt_table.cpp
	$(CXX) -std=c++20 -O2 -Wall -Wextra $(patsubst -I%,-isystem %,$(shell pkg-config --cflags arrow parquet simdjson libzstd)) tools/receipt_table.cpp \
	  $(shell pkg-config --libs arrow parquet simdjson libzstd) -lsqlite3 -o $@

test-pack: bonfyre-project
	sh tests/receipt_pack_test.sh ./bonfyre-project

clean:
	rm -f bonfyre-project bonfyre-receipt-table

.PHONY: http-envelope-witness
http-envelope-witness: bonfyre-project
	./tests/http_envelope_test.sh ./bonfyre-project
	./tests/retained_body_settle_test.sh ./bonfyre-project

.PHONY: credentialed-reheat-witness
credentialed-reheat-witness: bonfyre-project
	./tests/credentialed_reheat_test.sh ./bonfyre-project

.PHONY: activation-projection-witness
activation-projection-witness: bonfyre-project
	./tests/activation_projection_test.sh ./bonfyre-project

.PHONY: foreign-release-projection-witness
foreign-release-projection-witness: bonfyre-project
	./tests/foreign_release_projection_test.sh ./bonfyre-project

.PHONY: destination-release-witness
destination-release-witness: bonfyre-project
	./tests/destination_release_test.sh ./bonfyre-project

.PHONY: discovery-set-witness
discovery-set-witness: bonfyre-project
	./tests/discovery_set_test.sh ./bonfyre-project

.PHONY: missing-cells-witness
missing-cells-witness: bonfyre-project
	./tests/missing_cells_test.sh ./bonfyre-project

.PHONY: test
test: foreign-release-projection-witness destination-release-witness discovery-set-witness missing-cells-witness bounded-sample-witness served-answer-witness economic-entrypoints-witness message-review-witness shared-conversation-witness

.PHONY: shared-conversation-witness
shared-conversation-witness:
	bun test ./tests/shared-conversation.test.mjs

.PHONY: message-review-witness
message-review-witness: bonfyre-project
	bun test ./tests/message_review.test.mjs

.PHONY: economic-entrypoints-witness
economic-entrypoints-witness:
	$(MAKE) -C ../../cmd/BonfyreLex
	bun test ./tests/economic_reports.test.mjs ./tests/economic_views_test.mjs \
		./tests/read_protocol_drive.test.mjs ./tests/project_liveness_test.mjs ./tests/project_demand_test.mjs

.PHONY: served-answer-witness
served-answer-witness:
	bun test ./tests/serve_native_test.mjs

.PHONY: bounded-sample-witness
bounded-sample-witness: bonfyre-project
	bun test ./tests/foreign_bounded_sample_test.mjs

.PHONY: foreign-twin-workspace-witness
foreign-twin-workspace-witness:
	../../site/matrix/surfaces/build/link_workspace_test.sh
