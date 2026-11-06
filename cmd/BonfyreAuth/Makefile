CC      ?= cc
CFLAGS  ?= -O2 -Wall -Wextra -std=c11 -ffunction-sections -fdata-sections -I../../lib/libbonfyre/include -I../../engine/core/include
LDFLAGS ?= ../../lib/libbonfyre/libbonfyre.a -lsqlite3
TARGET   = bonfyre-auth

.PHONY: all clean test

all: $(TARGET)

$(TARGET): src/main.c src/institution_members.h src/organization_workspace.h src/session_handoff.h src/background_access.h ../BonfyreCMS/src/household_access.h
	$(CC) $(CFLAGS) -Wl,-dead_strip -o $@ $< $(LDFLAGS)

clean:
	rm -f $(TARGET)

test: $(TARGET)
	python3 tests/background_access_test.py
	./tests/institution_grant_test.sh
	python3 tests/institution_members_test.py
	bun test tests/organization_workspace.test.mjs
	./tests/actor_closure_test.sh
	./tests/external_account_link_test.sh
