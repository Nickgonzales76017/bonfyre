CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
LDLIBS = -lsqlite3
TARGET = bonfyre-warehouse-seed
SRC = src/main.c

.PHONY: all test clean
all: $(TARGET)
$(TARGET): $(SRC) ../BonfyreWarehouse/src/warehouse_seed.h
	$(CC) $(CFLAGS) -o $@ $< $(LDLIBS)

test: $(TARGET)
	$(MAKE) -C ../BonfyreWarehouse test

clean:
	rm -f $(TARGET)
