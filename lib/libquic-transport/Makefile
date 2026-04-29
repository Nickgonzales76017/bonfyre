CC ?= cc
CFLAGS ?= -O3 -march=native -flto=auto -Wall -Wextra -std=c11
BREW := /opt/homebrew

CFLAGS += -I$(BREW)/include -Iinclude
LDFLAGS = -L$(BREW)/lib -lngtcp2 -lngtcp2_crypto_ossl -lssl -lcrypto

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
