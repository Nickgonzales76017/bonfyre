CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11 -D_DARWIN_C_SOURCE
ROOT := ../..
TARGET = bonfyre-release-approval

all: $(TARGET) test

$(TARGET): src/main.c $(ROOT)/lib/libbonfyre/bf_jsonw.c $(ROOT)/lib/libbonfyre/bf_json.c $(ROOT)/lib/libbonfyre/src/bf_sha256.c
	$(CC) $(CFLAGS) -I$(ROOT)/lib/libbonfyre/include -o $@ src/main.c $(ROOT)/lib/libbonfyre/bf_jsonw.c $(ROOT)/lib/libbonfyre/bf_json.c $(ROOT)/lib/libbonfyre/src/bf_sha256.c -lsqlite3

test: $(TARGET)
	sh tests/release_approval_test.sh ./$(TARGET)
	sh $(ROOT)/scripts/tests/organ_drift_test.sh
	bun test $(ROOT)/scripts/release/tests/public_surface_rollout.test.mjs
	bun test $(ROOT)/scripts/tests/cloudflare-reconcile.test.mjs
	bun test $(ROOT)/integrations/browser/tests/aurekai-activity-interaction.browser.test.mjs
	$(MAKE) -C $(ROOT)/cmd/BonfyreWebAddress community-journey-witness
	bun test $(ROOT)/cmd/BonfyreWebAddress/tests/account_inbox_web.test.mjs
	bun test $(ROOT)/cmd/BonfyreWebAddress/tests/account_activity_chrome.test.mjs

clean:
	rm -f $(TARGET)

.PHONY: all test clean
