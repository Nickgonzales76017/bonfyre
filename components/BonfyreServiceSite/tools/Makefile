CC ?= cc
LIB = ../../../lib/libbonfyre
CFLAGS ?= -O2 -Wall -Wextra
CFLAGS += -std=c11 -DBONFYRE_SHA256_PORTABLE -I$(LIB)/include

all: product-review

product-review: product-review.c
	$(CC) $(CFLAGS) -o $@ product-review.c $(LIB)/bf_json.c $(LIB)/bf_jsonw.c $(LIB)/src/bf_sha256.c

# Prints through the real headless Chrome (BONFYRE_CHROME overrides the path).
test: product-review
	sh product-review-test.sh ./product-review

clean:
	rm -f product-review

.PHONY: all test clean
