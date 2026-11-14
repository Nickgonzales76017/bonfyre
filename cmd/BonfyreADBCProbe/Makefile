CC ?= cc
CFLAGS ?= -O2 -std=c11 -Wall -Wextra -Werror
# adbc.h ships with the ADBC driver manager; its pkg-config entry names the
# include directory. An explicit ADBC_INCLUDE still wins, and a machine without
# the driver manager still stops with the message below.
ADBC_INCLUDE ?= $(shell pkg-config --variable=includedir adbc-driver-manager 2>/dev/null)

.PHONY: all clean
all: bonfyre-adbc-probe

bonfyre-adbc-probe: src/main.c
	@test -n "$(ADBC_INCLUDE)" || (echo "ADBC_INCLUDE is required (install apache-arrow-adbc or set ADBC_INCLUDE)" >&2; exit 2)
	$(CC) $(CFLAGS) -I$(ADBC_INCLUDE) -o $@ $<

clean:
	$(RM) bonfyre-adbc-probe
