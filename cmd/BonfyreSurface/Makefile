CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
CFLAGS += -I../BonfyreLex/src
TARGET = bonfyre-surface
SRC = src/main.c
IR_TARGET = bonfyre-surface-ir
IR_SRC = src/surface_ir_main.c src/surface_ir.c
IR_HDR = src/surface_ir.h

all: $(TARGET) $(IR_TARGET) test-ir

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC)

$(IR_TARGET): $(IR_SRC) $(IR_HDR)
	$(CC) $(CFLAGS) -Isrc -o $(IR_TARGET) $(IR_SRC) -lsqlite3

test-ir: $(IR_TARGET)
	@sh tests/site_fragment_surface_ir_test.sh

clean:
	rm -f $(TARGET) $(IR_TARGET)

.PHONY: all clean test-ir
