CC      ?= cc
CFLAGS  ?= -O2 -Wall -Wextra -std=c11
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LDFLAGS  = ../../lib/libbonfyre/libbonfyre.a -lsqlite3
TARGET   = bonfyre-query
SRC      = src/main.c

all: $(TARGET) prioritization-check

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

clean:
	rm -f $(TARGET)

test: $(TARGET) prioritization-check
	./tests/sql_rows_test.sh ./$(TARGET)

prioritization-check: $(TARGET)
	BONFYRE_QUERY_TEST_BINARY="$(CURDIR)/$(TARGET)" node --test tests/prioritization_test.mjs

.PHONY: all test clean prioritization-check
