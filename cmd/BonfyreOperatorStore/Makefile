CC ?= cc
LIB = ../../lib/libbonfyre
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11 -D_DARWIN_C_SOURCE
CFLAGS += -I$(LIB)/include
LDFLAGS ?= -L$(LIB) -lbonfyre -lsqlite3
TARGET = bonfyre-operator-store

.PHONY: all clean test

all: $(TARGET)

$(LIB)/libbonfyre.a:
	$(MAKE) -C $(LIB)

$(TARGET): src/main.c $(LIB)/libbonfyre.a
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

test: $(TARGET)
	sh ./tests/operator_store_test.sh ./$(TARGET)
	sh ./tests/route_test.sh ./$(TARGET)
	sh ./tests/quality_record_test.sh ./$(TARGET)
	sh ./tests/backlog_audit_test.sh ./$(TARGET)

clean:
	rm -f $(TARGET)
