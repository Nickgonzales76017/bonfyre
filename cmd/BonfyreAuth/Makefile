CC      ?= cc
CFLAGS  ?= -O2 -Wall -Wextra -std=c11 -ffunction-sections -fdata-sections -I../../lib/libbonfyre/include -I../../engine/core/include
LDFLAGS ?= ../../lib/libbonfyre/libbonfyre.a -lsqlite3 -lm
TARGET   = bonfyre-auth
# Mach-O and ELF use different section-garbage-collection linker flags.
DEAD_STRIP := -Wl,--gc-sections
ifeq ($(shell uname -s),Darwin)
DEAD_STRIP := -Wl,-dead_strip
endif

.PHONY: all clean test

all: $(TARGET)

$(TARGET): src/main.c src/institution_members.h src/organization_workspace.h src/session_handoff.h src/background_access.h ../BonfyreCMS/src/household_access.h
	$(CC) $(CFLAGS) $(DEAD_STRIP) -o $@ $< $(LDFLAGS)

clean:
	rm -f $(TARGET)

test: $(TARGET)
	sh tests/background_access_test.sh
	./tests/institution_grant_test.sh
	sh tests/institution_members_test.sh
	bun test tests/organization_workspace.test.mjs
	./tests/actor_closure_test.sh
	./tests/external_account_link_test.sh
