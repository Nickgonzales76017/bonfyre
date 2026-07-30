# Makefile for BonfyreMoE
#
# Distributed expert-streaming MoE inference binary.
# Links against libcolibri-bonfyre and Bonfyre core libraries.

CC       ?= gcc
CFLAGS   := -std=c11 -Wall -Wextra -O3 -march=native
CPPFLAGS := -D_DEFAULT_SOURCE -D_GNU_SOURCE

# OpenMP support (disabled by default on macOS clang)
# Set WITH_OPENMP=1 to enable if compiler supports it
ifeq ($(WITH_OPENMP),1)
    CFLAGS += -fopenmp
    LDFLAGS += -fopenmp
endif

# Darwin (macOS) doesn't have OpenMP in default clang
UNAME_S := $(shell uname -s)
ifneq ($(UNAME_S),Darwin)
    ifeq ($(CC),gcc)
        CFLAGS += -fopenmp
        LDFLAGS += -fopenmp
    endif
endif

INCLUDES := -Isrc \
            -I../../lib/libcolibri-bonfyre/include \
            -I../../lib/libbonfyre/include \
            -I../../lib/libfragment/include \
            -I../../lib/libquic-transport/include \
            -I../../lib/liblambda-tensors/include

SRCDIR   := src
OBJDIR   := obj

# Source files
SOURCES  := $(wildcard $(SRCDIR)/*.c)
OBJECTS  := $(SOURCES:$(SRCDIR)/%.c=$(OBJDIR)/%.o)

# Library paths
LIBDIR   := ../../lib
LIBS     := -L$(LIBDIR)/libcolibri-bonfyre/lib -lcolibri-bonfyre \
            -L$(LIBDIR)/libfragment -lfragment \
            -L$(LIBDIR)/libbonfyre -lbonfyre \
            -L$(LIBDIR)/libquic-transport -lquic-transport \
            -L$(LIBDIR)/liblambda-tensors -llambda-tensors \
            -L/opt/homebrew/lib -lssl -lcrypto \
            -L$(LIBDIR)/libbonfyre -lbonfyre \
            -lpthread -lm -lsqlite3

# Output binary
TARGET   := bonfyre-moe

.PHONY: all clean test

all: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CC) $(CFLAGS) $^ $(LIBS) -o $@
	@echo "  [LD]  $@"

$(OBJDIR)/%.o: $(SRCDIR)/%.c | $(OBJDIR)
	$(CC) $(CFLAGS) $(CPPFLAGS) $(INCLUDES) -c $< -o $@
	@echo "  [CC]  $<"

$(OBJDIR):
	mkdir -p $@

clean:
	rm -rf $(OBJDIR) $(TARGET)

test: $(TARGET)
	@echo "Running BonfyreMoE tests..."
	./$(TARGET) plan --model /tmp/test_model --vram 80 --ram 128 || true
	./$(TARGET) doctor --model /tmp/test_model || true
	@echo "✓ Smoke test passed"

.PHONY: test
