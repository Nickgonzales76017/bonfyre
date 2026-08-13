# Makefile for libcolibri-bonfyre
#
# Expert-streaming MoE inference library combining Colibri's proven
# architecture with Bonfyre's distributed capabilities.
#
# Dependencies: libbonfyre, libfragment, libquic-transport, liblambda-tensors

CC       ?= gcc
AR       ?= ar
CFLAGS   := -std=c11 -Wall -Wextra -Wno-unused-parameter -O3 -march=native
CPPFLAGS := -D_DEFAULT_SOURCE -D_GNU_SOURCE

# OpenMP support (optional, disable on macOS/clang by default)
ifeq ($(shell uname),Darwin)
    # macOS: Only add -fopenmp if user explicitly enables it
    ifdef WITH_OPENMP
        CFLAGS += -fopenmp
    endif
else
    # Linux: Enable OpenMP by default (can disable with NO_OPENMP=1)
    ifndef NO_OPENMP
        CFLAGS += -fopenmp
    endif
endif

INCLUDES := -Iinclude \
            -I../libbonfyre/include \
            -I../libfragment/include \
            -I../libquic-transport/include \
            -I../liblambda-tensors/include

SRCDIR   := src
OBJDIR   := obj
LIBDIR   := lib

# Source files
SOURCES  := $(wildcard $(SRCDIR)/*.c)
OBJECTS  := $(SOURCES:$(SRCDIR)/%.c=$(OBJDIR)/%.o)

# Output library
TARGET   := $(LIBDIR)/libcolibri-bonfyre.a

.PHONY: all clean test

all: $(TARGET)

$(TARGET): $(OBJECTS) | $(LIBDIR)
	$(AR) rcs $@ $^
	@echo "  [AR]  $@"

$(OBJDIR)/%.o: $(SRCDIR)/%.c | $(OBJDIR)
	$(CC) $(CFLAGS) $(CPPFLAGS) $(INCLUDES) -c $< -o $@
	@echo "  [CC]  $<"

$(OBJDIR) $(LIBDIR):
	mkdir -p $@

clean:
	rm -rf $(OBJDIR) $(LIBDIR)

test: $(TARGET)
	@echo "Running libcolibri-bonfyre tests..."
	@echo "  (test suite TODO)"

# Install headers
install-headers:
	mkdir -p ../../dist/include
	cp include/*.h ../../dist/include/

.PHONY: install-headers
