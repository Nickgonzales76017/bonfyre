CC ?= cc
CFLAGS ?= -O3 -march=native -flto=auto -Wall -Wextra -std=c11

# The pkg-config modules are named libngtcp2 / libngtcp2_crypto_ossl -- the
# unprefixed spellings never matched anything, so this always took the Homebrew
# fallback and the archive was built against whatever headers happened to be at
# /opt/homebrew/include. That is a macOS-only path, so on Linux the include
# resolved to nothing and the fallback was silent either way. Ask for the real
# module names, and fall back only when Homebrew is actually present.
NGTCP2_PC := libngtcp2 libngtcp2_crypto_ossl
NGTCP2_CFLAGS := $(shell pkg-config --cflags $(NGTCP2_PC) 2>/dev/null)
NGTCP2_LIBS   := $(shell pkg-config --libs   $(NGTCP2_PC) openssl 2>/dev/null)
ifeq ($(strip $(NGTCP2_LIBS)),)
  ifneq ($(wildcard /opt/homebrew/lib/libngtcp2.*),)
    NGTCP2_CFLAGS := -I/opt/homebrew/include
    NGTCP2_LIBS   := -L/opt/homebrew/lib -lngtcp2 -lngtcp2_crypto_ossl -lssl -lcrypto
  else
    $(error QUIC transport needs libngtcp2 + libngtcp2_crypto_ossl; set PKG_CONFIG_PATH)
  endif
endif

# -std=c11 defines __STRICT_ANSI__, which hides everything POSIX behind feature
# macros on glibc: CLOCK_MONOTONIC and struct addrinfo both vanish. macOS headers
# expose them regardless, so this only ever broke the Linux build.
CFLAGS += -D_GNU_SOURCE
CFLAGS += $(NGTCP2_CFLAGS) -Iinclude
LDFLAGS = $(NGTCP2_LIBS)

SRCS = bf_quic.c
OBJS = $(SRCS:.c=.o)
LIB  = libquic-transport.a

all: $(LIB)

TEST_BIN = tests/operation_phenotype_test

$(LIB): $(OBJS)
	ar rcs $@ $^

%.o: %.c include/bf_quic.h
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(LIB) $(TEST_BIN)

$(TEST_BIN): tests/operation_phenotype_test.c $(LIB) include/bf_quic.h
	$(CC) $(CFLAGS) -o $@ tests/operation_phenotype_test.c $(LIB) $(LDFLAGS)

test: $(TEST_BIN)
	./$(TEST_BIN)

.PHONY: all clean test
