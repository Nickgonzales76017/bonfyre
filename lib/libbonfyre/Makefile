# libbonfyre — shared Bonfyre runtime library

CC      ?= cc
OPTFLAGS ?= -O2 -Wall -Wextra -std=c11
CFLAGS   = $(OPTFLAGS) -I$(INCDIR)
AR      ?= ar
RANLIB  ?= ranlib

SRCDIR  = src
INCDIR  = include
OBJDIR  = obj

SRCS    = $(SRCDIR)/bf_artifact.c \
          $(SRCDIR)/bf_common.c \
          $(SRCDIR)/bf_discipl.c \
          $(SRCDIR)/bf_layer_artifact.c \
          $(SRCDIR)/bf_catalog.c \
          $(SRCDIR)/bf_sha256.c \
          $(SRCDIR)/bf_operators.c

# Batch 1+2 modules (in project root, not src/)
EXT_SRCS = bf_reactor.c \
           bf_hotload.c \
           bf_fountain.c \
           bf_textfuse.c \
           bf_lmdb.c \
           bf_pipeline_coro.c \
           bf_flatbuf.c \
           bf_picohttpparser.c \
           bf_json.c \
           bf_crypto.c \
           bf_bloom.c \
           bf_dns.c \
           bf_log.c \
           bf_mmap.c \
           bf_msgpack.c \
           bf_opts.c \
           bf_pool.c \
           bf_utf8.c \
           bf_conf.c \
           bf_lz4.c \
           bf_hashmap.c \
           bf_csv.c \
           bf_sqlite.c

OBJS    = $(patsubst $(SRCDIR)/%.c,$(OBJDIR)/%.o,$(SRCS))
EXT_OBJS = $(patsubst %.c,$(OBJDIR)/%.o,$(EXT_SRCS))
TARGET  = libbonfyre.a
SHARED  = libbonfyre.so

.PHONY: all clean install test

all: $(TARGET)

$(OBJDIR)/%.o: $(SRCDIR)/%.c $(INCDIR)/bonfyre.h | $(OBJDIR)
	$(CC) $(CFLAGS) -I$(INCDIR) -c $< -o $@

$(OBJDIR)/%.o: %.c | $(OBJDIR)
	$(CC) $(CFLAGS) -I$(INCDIR) -c $< -o $@

$(OBJDIR):
	mkdir -p $(OBJDIR)

$(TARGET): $(OBJS) $(EXT_OBJS)
	$(AR) rcs $@ $^
	$(RANLIB) $@

$(SHARED): $(OBJS) $(EXT_OBJS)
	$(CC) -shared -o $@ $^ -lpthread

clean:
	rm -rf $(OBJDIR) $(TARGET) $(SHARED) test_bonfyre

test: $(TARGET)
	$(CC) $(CFLAGS) -I$(INCDIR) test/test_bonfyre.c -L. -lbonfyre -o test_bonfyre
	./test_bonfyre

install: $(TARGET)
	install -d $(DESTDIR)$(PREFIX)/lib
	install -d $(DESTDIR)$(PREFIX)/include
	install -d $(DESTDIR)$(PREFIX)/include/bonfyre
	install -m 644 $(TARGET) $(DESTDIR)$(PREFIX)/lib/
	install -m 644 $(INCDIR)/bonfyre.h $(DESTDIR)$(PREFIX)/include/
	install -m 644 $(INCDIR)/bonfyre/bf_discipl.h $(DESTDIR)$(PREFIX)/include/bonfyre/
	install -m 644 $(INCDIR)/bf_reactor.h $(DESTDIR)$(PREFIX)/include/ 2>/dev/null || true
	install -m 644 $(INCDIR)/bf_hotload.h $(DESTDIR)$(PREFIX)/include/ 2>/dev/null || true
	install -m 644 $(INCDIR)/bf_fountain.h $(DESTDIR)$(PREFIX)/include/ 2>/dev/null || true
	install -m 644 $(INCDIR)/bf_textfuse.h $(DESTDIR)$(PREFIX)/include/ 2>/dev/null || true
	install -m 644 $(INCDIR)/bf_lmdb.h $(DESTDIR)$(PREFIX)/include/ 2>/dev/null || true
	install -m 644 $(INCDIR)/bf_pipeline_coro.h $(DESTDIR)$(PREFIX)/include/ 2>/dev/null || true
	install -m 644 $(INCDIR)/bf_flatbuf.h $(DESTDIR)$(PREFIX)/include/ 2>/dev/null || true
	install -m 644 $(INCDIR)/bf_picohttpparser.h $(DESTDIR)$(PREFIX)/include/ 2>/dev/null || true
	install -m 644 $(INCDIR)/bf_json.h $(DESTDIR)$(PREFIX)/include/ 2>/dev/null || true
	install -m 644 $(INCDIR)/bf_crypto.h $(DESTDIR)$(PREFIX)/include/ 2>/dev/null || true
	install -m 644 $(INCDIR)/bf_bloom.h $(DESTDIR)$(PREFIX)/include/ 2>/dev/null || true
	install -m 644 $(INCDIR)/bf_dns.h $(DESTDIR)$(PREFIX)/include/ 2>/dev/null || true
	install -m 644 $(INCDIR)/bf_log.h $(DESTDIR)$(PREFIX)/include/ 2>/dev/null || true
	install -m 644 $(INCDIR)/bf_mmap.h $(DESTDIR)$(PREFIX)/include/ 2>/dev/null || true
	install -m 644 $(INCDIR)/bf_msgpack.h $(DESTDIR)$(PREFIX)/include/ 2>/dev/null || true
	install -m 644 $(INCDIR)/bf_opts.h $(DESTDIR)$(PREFIX)/include/ 2>/dev/null || true
	install -m 644 $(INCDIR)/bf_pool.h $(DESTDIR)$(PREFIX)/include/ 2>/dev/null || true
	install -m 644 $(INCDIR)/bf_utf8.h $(DESTDIR)$(PREFIX)/include/ 2>/dev/null || true
	install -m 644 $(INCDIR)/bf_conf.h $(DESTDIR)$(PREFIX)/include/ 2>/dev/null || true
	install -m 644 $(INCDIR)/bf_lz4.h $(DESTDIR)$(PREFIX)/include/ 2>/dev/null || true
	install -m 644 $(INCDIR)/bf_hashmap.h $(DESTDIR)$(PREFIX)/include/ 2>/dev/null || true
	install -m 644 $(INCDIR)/bf_csv.h $(DESTDIR)$(PREFIX)/include/ 2>/dev/null || true
