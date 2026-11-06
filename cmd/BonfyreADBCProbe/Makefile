CC ?= cc
CFLAGS ?= -O2 -std=c11 -Wall -Wextra -Werror
ADBC_INCLUDE ?=

.PHONY: all clean
all: bonfyre-adbc-probe

bonfyre-adbc-probe: src/main.c
	@test -n "$(ADBC_INCLUDE)" || (echo "ADBC_INCLUDE is required" >&2; exit 2)
	$(CC) $(CFLAGS) -I$(ADBC_INCLUDE) -o $@ $<

clean:
	$(RM) bonfyre-adbc-probe
