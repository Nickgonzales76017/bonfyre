CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
.PHONY: test sanitizer-test
test: build/native-publications-query-test
	./build/native-publications-query-test
build/native-publications-query-test: tests/native_publications_query_test.c src/native_publications_query.h
	@mkdir -p build
	$(CC) $(CFLAGS) tests/native_publications_query_test.c -lsqlite3 -o $@
sanitizer-test: build/native-publications-query-sanitizer-test
	./build/native-publications-query-sanitizer-test
build/native-publications-query-sanitizer-test: tests/native_publications_query_test.c src/native_publications_query.h
	@mkdir -p build
	$(CC) -O1 -g -Wall -Wextra -Werror -std=c11 -fsanitize=address,undefined -fno-omit-frame-pointer tests/native_publications_query_test.c -lsqlite3 -o $@
