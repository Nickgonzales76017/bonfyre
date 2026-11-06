CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
LOCAL_CFLAGS = -I../../lib/libbonfyre/include -D_DARWIN_C_SOURCE -D_DEFAULT_SOURCE
LDFLAGS  = -L../../lib/libbonfyre -lbonfyre -lsqlite3 -lm
TARGET = bonfyre-brief
SRC = src/main.c src/cited.c src/evidence.c src/inventory.c

all: $(TARGET)

$(TARGET): $(SRC) src/evidence.h
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

# Receipt-bound migration briefs over a throwaway CMS, receipts and WorkGraph.
test: $(TARGET)
	sh tests/cited_brief_test.sh ./$(TARGET)
	sh tests/hosting_inventory_test.sh ./$(TARGET)
	# The practice site needs a web-address binary; it skips without one.
	sh tests/practice_site_test.sh $${BONFYRE_TEST_WEB_BINARY:-}

clean:
	rm -f $(TARGET)

.PHONY: all clean test
