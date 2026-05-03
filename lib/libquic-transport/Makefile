CC ?= cc
CFLAGS ?= -O3 -march=native -flto=auto -Wall -Wextra -std=c11

NGTCP2_CFLAGS := $(shell pkg-config --cflags ngtcp2 ngtcp2_crypto_ossl 2>/dev/null || echo "-I/opt/homebrew/include")
NGTCP2_LIBS   := $(shell pkg-config --libs   ngtcp2 ngtcp2_crypto_ossl 2>/dev/null || \
                  echo "-L/opt/homebrew/lib -lngtcp2 -lngtcp2_crypto_ossl -lssl -lcrypto")

CFLAGS += $(NGTCP2_CFLAGS) -Iinclude
LDFLAGS = $(NGTCP2_LIBS)

SRCS = bf_quic.c
OBJS = $(SRCS:.c=.o)
LIB  = libquic-transport.a

all: $(LIB)

$(LIB): $(OBJS)
	ar rcs $@ $^

%.o: %.c include/bf_quic.h
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(LIB)

.PHONY: all clean
