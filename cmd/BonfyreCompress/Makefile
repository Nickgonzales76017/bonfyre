CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11 -ffunction-sections -fdata-sections
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LIB = ../../lib/libbonfyre/libbonfyre.a
TARGET = bonfyre-compress
SRC = src/main.c src/physical.c src/record_factor.c src/record_factor_cli.c
ZSTD_PREFIX ?= $(shell for p in /opt/homebrew /opt/local /usr/local /usr; do [ -f $$p/include/zstd.h ] && echo $$p && break; done)
ifneq ($(ZSTD_PREFIX),)
LOCAL_CFLAGS += -DBONFYRE_HAVE_ZSTD -I$(ZSTD_PREFIX)/include
ZSTD_LIBS = $(if $(wildcard $(ZSTD_PREFIX)/lib/libzstd.a),$(ZSTD_PREFIX)/lib/libzstd.a,-L$(ZSTD_PREFIX)/lib -lzstd)
endif

all: $(TARGET)

$(LIB): force-library
	$(MAKE) -C ../../lib/libbonfyre

$(TARGET): $(SRC) src/record_factor.h $(LIB)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -Wl,-dead_strip -o $(TARGET) $(SRC) $(LIB) $(LDFLAGS) -lsqlite3 $(ZSTD_LIBS)

clean:
	rm -f $(TARGET)

.PHONY: all clean test force-library

test: $(TARGET)
	sh ./tests/physical_compiler_test.sh ./$(TARGET)
	sh ./tests/family_pipeline_test.sh ./$(TARGET)
	sh ./tests/record_factor_test.sh ./$(TARGET)
	bash ./tests/savings_accounting_test.sh ./$(TARGET)
ifneq ($(ZSTD_PREFIX),)
	bun test ./tests/savings_stream.test.mjs
endif
