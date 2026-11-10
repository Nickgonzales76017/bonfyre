# Independent witness. Does not rebuild or alter the WebAddress executable.
CC ?= cc
CPPFLAGS += -I../../lib/libbonfyre/include -I../BonfyreLex/src -I../BonfyreCampaign/src
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
SOURCES = tests/campaign_workspace_test.c ../BonfyreCampaign/src/campaign.c ../../lib/libbonfyre/bf_json.c ../../lib/libbonfyre/bf_jsonw.c ../../lib/libbonfyre/bf_utf8.c ../../lib/libbonfyre/src/bf_sha256.c
HEADERS = src/campaign_workspace.h ../BonfyreCampaign/src/campaign.h ../BonfyreLex/src/bf_lex.h
.PHONY: test sanitizer-test
test: build/campaign-workspace-test
	./build/campaign-workspace-test
build/campaign-workspace-test: $(SOURCES) $(HEADERS)
	@mkdir -p build
	$(CC) $(CFLAGS) $(CPPFLAGS) $(SOURCES) -lm -o $@
sanitizer-test: build/campaign-workspace-sanitizer-test
	./build/campaign-workspace-sanitizer-test
build/campaign-workspace-sanitizer-test: $(SOURCES) $(HEADERS)
	@mkdir -p build
	$(CC) -O1 -g -Wall -Wextra -Werror -std=c11 -fsanitize=address,undefined -fno-omit-frame-pointer $(CPPFLAGS) $(SOURCES) -lm -o $@
