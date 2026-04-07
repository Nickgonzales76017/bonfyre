CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LDFLAGS  = -L../../lib/libbonfyre -lbonfyre
TARGET = bonfyre-cms
COMPACT_PROBE = tests/compact-probe

# liblambda-tensors (standalone compression library)
LT_DIR = ../../lib/liblambda-tensors
LT_INC = -I$(LT_DIR)/include
LT_LIB = $(LT_DIR)/liblambda-tensors.a

SRC = src/main.c src/bench_metrics.c src/ephemeral.c src/canonical.c src/lt_egraph.c src/tensor_ops.c \
      src/incremental.c src/synthesis.c src/ann_index.c src/delta_sync.c \
      src/type_check.c src/meta_gen.c src/columnar.c src/hybrid_reduce.c \
      src/provenance.c src/transfer.c src/compact_bindings.c

HDR = src/bench_metrics.h src/ephemeral.h src/canonical.h src/lt_egraph.h src/tensor_ops.h \
      src/incremental.h src/synthesis.h src/ann_index.h src/delta_sync.h \
      src/type_check.h src/meta_gen.h src/columnar.h src/hybrid_reduce.h \
      src/provenance.h src/transfer.h src/compact_bindings.h

all: $(LT_LIB) $(TARGET)

$(LT_LIB):
	$(MAKE) -C $(LT_DIR) liblambda-tensors.a

$(TARGET): $(SRC) $(HDR) $(LT_LIB)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) $(LT_INC) -o $(TARGET) $(SRC) $(LT_LIB) -lsqlite3 -lpthread -lm -lz $(LDFLAGS)

$(COMPACT_PROBE): tests/compact_probe.c src/compact_bindings.c src/compact_bindings.h src/bench_metrics.c src/bench_metrics.h $(LT_LIB)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -Isrc $(LT_INC) -o $(COMPACT_PROBE) tests/compact_probe.c src/compact_bindings.c src/bench_metrics.c $(LT_LIB) -lsqlite3 -lm

compact-probe: $(COMPACT_PROBE)

clean:
	rm -f $(TARGET) $(COMPACT_PROBE)

PREFIX ?= /usr/local
install: $(TARGET)
	install -d $(DESTDIR)$(PREFIX)/bin
	install -m 755 $(TARGET) $(DESTDIR)$(PREFIX)/bin/$(TARGET)

uninstall:
	rm -f $(DESTDIR)$(PREFIX)/bin/$(TARGET)

.PHONY: all clean compact-probe install uninstall
