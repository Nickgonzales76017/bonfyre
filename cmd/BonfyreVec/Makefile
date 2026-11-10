CC      ?= cc
CFLAGS  ?= -O2 -Wall -Wextra -std=c11
CFLAGS += -I../BonfyreLex/src
SQLITE_PREFIX  := $(shell brew --prefix sqlite 2>/dev/null)
SQLITE_CFLAGS  := $(shell pkg-config --cflags sqlite3 2>/dev/null) $(if $(SQLITE_PREFIX),-I$(SQLITE_PREFIX)/include,-I/opt/homebrew/opt/sqlite/include)
SQLITE_LIBS    := $(shell pkg-config --libs sqlite3 2>/dev/null) $(if $(SQLITE_PREFIX),-L$(SQLITE_PREFIX)/lib,-L/opt/homebrew/opt/sqlite/lib) -lsqlite3
LOCAL_CFLAGS = $(SQLITE_CFLAGS) -DSQLITE_ENABLE_LOAD_EXTENSION
LDFLAGS  = $(SQLITE_LIBS) -lm  # libm is implicit in macOS libSystem; Linux needs it explicit
TARGET   = bonfyre-vec
SRC      = src/main.c

all: $(TARGET)

test: $(TARGET)
	bash tests/width_admission_test.sh
	sh tests/delete_test.sh

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

MANAGED_PREFIX ?= $(HOME)/.bonfyre
install: all
	install -d "$(MANAGED_PREFIX)/bin"
	install -m 755 $(TARGET) "$(MANAGED_PREFIX)/bin/.bonfyre-vec.next"
	mv -f "$(MANAGED_PREFIX)/bin/.bonfyre-vec.next" "$(MANAGED_PREFIX)/bin/bonfyre-vec"

clean:
	rm -f $(TARGET)

.PHONY: all clean test install
