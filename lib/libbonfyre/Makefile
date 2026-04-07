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
          $(SRCDIR)/bf_sha256.c \
          $(SRCDIR)/bf_operators.c

OBJS    = $(patsubst $(SRCDIR)/%.c,$(OBJDIR)/%.o,$(SRCS))
TARGET  = libbonfyre.a
SHARED  = libbonfyre.so

.PHONY: all clean install test

all: $(TARGET)

$(OBJDIR)/%.o: $(SRCDIR)/%.c $(INCDIR)/bonfyre.h | $(OBJDIR)
	$(CC) $(CFLAGS) -I$(INCDIR) -c $< -o $@

$(OBJDIR):
	mkdir -p $(OBJDIR)

$(TARGET): $(OBJS)
	$(AR) rcs $@ $^
	$(RANLIB) $@

$(SHARED): $(OBJS)
	$(CC) -shared -o $@ $^

clean:
	rm -rf $(OBJDIR) $(TARGET) $(SHARED) test_bonfyre

test: $(TARGET)
	$(CC) $(CFLAGS) -I$(INCDIR) test/test_bonfyre.c -L. -lbonfyre -o test_bonfyre
	./test_bonfyre

install: $(TARGET)
	install -d $(DESTDIR)$(PREFIX)/lib
	install -d $(DESTDIR)$(PREFIX)/include
	install -m 644 $(TARGET) $(DESTDIR)$(PREFIX)/lib/
	install -m 644 $(INCDIR)/bonfyre.h $(DESTDIR)$(PREFIX)/include/
