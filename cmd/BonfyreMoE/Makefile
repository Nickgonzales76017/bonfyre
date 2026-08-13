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
            -lpthread -lm -lsqlite3

# Output binary
TARGET   := bonfyre-moe
SYNC_BIN := ../../bin/bonfyre-moe

.PHONY: all clean test install-bin

all: $(TARGET) install-bin

$(TARGET): $(OBJECTS)
	$(CC) $(CFLAGS) $^ $(LIBS) -o $@
	@echo "  [LD]  $@"

$(OBJDIR)/%.o: $(SRCDIR)/%.c | $(OBJDIR)
	$(CC) $(CFLAGS) $(CPPFLAGS) $(INCLUDES) -c $< -o $@
	@echo "  [CC]  $<"

$(OBJDIR):
	mkdir -p $@

install-bin: $(TARGET)
	mkdir -p ../../bin
	cp $(TARGET) $(SYNC_BIN)
	codesign -s - $(SYNC_BIN) >/dev/null 2>&1 || true
	mkdir -p $(HOME)/.bonfyre/bin
	cp $(TARGET) $(HOME)/.bonfyre/bin/$(TARGET)
	codesign -s - $(HOME)/.bonfyre/bin/$(TARGET) >/dev/null 2>&1 || true

clean:
	rm -rf $(OBJDIR) $(TARGET)

test: $(TARGET)
	@echo "Running BonfyreMoE tests..."
	@task_bin=$$(mktemp /tmp/bonfyre-moe-test.XXXXXX); \
	trap 'rm -f "$$task_bin"' EXIT; \
	cp ./$(TARGET) "$$task_bin"; \
	chmod +x "$$task_bin"; \
	if command -v codesign >/dev/null 2>&1; then codesign -s - "$$task_bin" >/dev/null 2>&1 || true; fi; \
	"$$task_bin" --help >/dev/null 2>&1; \
	"$$task_bin" status | grep -q '"schema":"bonfyre.moe.status.v1"'; \
	"$$task_bin" plan --model /tmp/test_model --arch mixtral --vram 8 --ram 16 --no-network >/dev/null
	@echo "✓ CLI, status, and placement-plan checks passed"

.PHONY: test
