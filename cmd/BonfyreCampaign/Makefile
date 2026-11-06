CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
CPPFLAGS += -I../../lib/libbonfyre/include -I../BonfyreLex/src -Isrc
NATIVE = ../../lib/libbonfyre/bf_json.c ../../lib/libbonfyre/bf_jsonw.c ../../lib/libbonfyre/bf_utf8.c ../../lib/libbonfyre/src/bf_sha256.c
HEADERS = src/campaign.h ../../lib/libbonfyre/include/bf_json.h ../../lib/libbonfyre/include/bf_jsonw.h ../../lib/libbonfyre/include/bf_utf8.h ../BonfyreLex/src/bf_lex.h
TARGET = build/bonfyre-campaign

.PHONY: all test sanitizer-test
all: $(TARGET)
$(TARGET): src/main.c src/campaign.c $(HEADERS) $(NATIVE)
	@mkdir -p build
	$(CC) $(CFLAGS) $(CPPFLAGS) src/main.c src/campaign.c $(NATIVE) -lm -o $@
build/campaign-test: tests/campaign_test.c src/campaign.c $(HEADERS) $(NATIVE)
	@mkdir -p build
	$(CC) $(CFLAGS) $(CPPFLAGS) tests/campaign_test.c src/campaign.c $(NATIVE) -lm -o $@
test: $(TARGET) build/campaign-test
	./build/campaign-test ./$(TARGET)
build/campaign-sanitizer-test: tests/campaign_test.c src/campaign.c $(HEADERS) $(NATIVE)
	@mkdir -p build
	$(CC) -O1 -g -Wall -Wextra -Werror -std=c11 -fsanitize=address,undefined -fno-omit-frame-pointer $(CPPFLAGS) tests/campaign_test.c src/campaign.c $(NATIVE) -lm -o $@
sanitizer-test: $(TARGET) build/campaign-sanitizer-test
	./build/campaign-sanitizer-test ./$(TARGET)
