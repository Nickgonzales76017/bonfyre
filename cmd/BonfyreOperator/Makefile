# bonfyre-operator: C port of the former Bun-compiled src/main.ts.
CC      ?= cc
LIB      = ../../lib/libbonfyre
CFLAGS  ?= -Os -Wall -Wextra -Werror -std=c11 -D_DEFAULT_SOURCE -D_DARWIN_C_SOURCE
CFLAGS  += -I$(LIB)/include -flto -ffunction-sections -fdata-sections
LDFLAGS += -flto
UNAME   := $(shell uname)
ifeq ($(UNAME),Darwin)
LDFLAGS += -Wl,-dead_strip
else
LDFLAGS += -Wl,--gc-sections
endif
TARGET   = bonfyre-operator

.PHONY: all clean test parity

all: $(TARGET)

$(LIB)/libbonfyre.a:
	$(MAKE) -C $(LIB)

$(TARGET): src/main.c $(LIB)/libbonfyre.a
	$(CC) $(CFLAGS) src/main.c $(LIB)/libbonfyre.a $(LDFLAGS) -lm -o $@

test: $(TARGET)
	bash ./tests/smoke.sh ./$(TARGET)

parity: $(TARGET)
	sh ./tests/parity.sh ./$(TARGET)

clean:
	rm -f $(TARGET)
