CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
ROOT := ../..
TARGET := bonfyre

.PHONY: all clean

all: $(ROOT)/engine/core/libbonfyre-fabric.a main.c
	$(CC) $(CFLAGS) -I$(ROOT)/engine/core/include main.c $(ROOT)/engine/core/libbonfyre-fabric.a -lsqlite3 -o $(TARGET)

$(ROOT)/engine/core/libbonfyre-fabric.a:
	$(MAKE) -C $(ROOT)/engine/core

clean:
	rm -f $(TARGET)
