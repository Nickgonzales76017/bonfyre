CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
LIBBONFYRE = ../../lib/libbonfyre
SOURCES = tests/program_continuation_test.c $(LIBBONFYRE)/bf_json.c $(LIBBONFYRE)/bf_jsonw.c $(LIBBONFYRE)/bf_msgpack.c $(LIBBONFYRE)/bf_utf8.c $(LIBBONFYRE)/src/bf_sha256.c
HEADERS = $(LIBBONFYRE)/include/bf_json.h $(LIBBONFYRE)/include/bf_jsonw.h $(LIBBONFYRE)/include/bf_msgpack.h $(LIBBONFYRE)/include/bf_utf8.h $(LIBBONFYRE)/include/bonfyre.h $(LIBBONFYRE)/include/bonfyre/bf_discipl.h
.PHONY: test sanitizer-test
test: build/program-continuation-test
	./build/program-continuation-test ../..
build/program-continuation-test: $(SOURCES) $(HEADERS)
	@mkdir -p build
	$(CC) $(CFLAGS) -I$(LIBBONFYRE)/include $(SOURCES) -lsqlite3 -lm -o $@
sanitizer-test: build/program-continuation-sanitizer-test
	./build/program-continuation-sanitizer-test ../..
build/program-continuation-sanitizer-test: $(SOURCES) $(HEADERS)
	@mkdir -p build
	$(CC) -O1 -g -Wall -Wextra -Werror -std=c11 -fsanitize=address,undefined -fno-omit-frame-pointer -I$(LIBBONFYRE)/include $(SOURCES) -lsqlite3 -lm -o $@
