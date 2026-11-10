# BonfyreFrappeCompiler Makefile

# Was hardcoded to clang, which does not exist on the Linux deploy host.

CC ?= cc
# A packager's CPPFLAGS and LDFLAGS are honoured, and a compiler newer than
# the one this was written against can build with WERROR= rather than fail on
# a warning it invented.
WERROR ?= -Werror
CFLAGS = -std=c11 -O2 -Wall -Wextra $(WERROR) -pedantic -D_POSIX_C_SOURCE=200809L -ffunction-sections -fdata-sections $(CPPFLAGS)
# gcc-only heuristic on snprintf into fixed buffers; probed, not name-matched.
CFLAGS += $(shell $(CC) -Wno-format-truncation -E -x c - </dev/null >/dev/null 2>&1 && echo -Wno-format-truncation)
CFLAGS += -I../BonfyreLex/src

LDLIBS = -lsqlite3 -lm

TARGET = bonfyre-frappe-compiler
SRC_DIR = src
BUILD_DIR = build
INSTALL_DIR = ../../bin

LIBBONFYRE_DIR = ../../lib/libbonfyre
LIBBONFYRE_INC = $(LIBBONFYRE_DIR)/include
LIBBONFYRE_SRC = $(LIBBONFYRE_DIR)/src

SOURCES = $(SRC_DIR)/main.c \
          $(LIBBONFYRE_SRC)/bonfyre_frappe_compiler.c \
          $(LIBBONFYRE_SRC)/bonfyre_frappe_condition.c \
          $(LIBBONFYRE_SRC)/bf_sha256.c

HEADERS = $(LIBBONFYRE_INC)/bonfyre_frappe.h \
          $(LIBBONFYRE_INC)/bonfyre_frappe_condition.h

OBJECTS = $(patsubst $(SRC_DIR)/%.c,$(BUILD_DIR)/%.o,$(filter $(SRC_DIR)/%.c,$(SOURCES))) \
          $(patsubst $(LIBBONFYRE_SRC)/%.c,$(BUILD_DIR)/%.o,$(filter $(LIBBONFYRE_SRC)/%.c,$(SOURCES))) \
          $(BUILD_DIR)/bf_jsonw.o $(BUILD_DIR)/bf_json.o

.PHONY: all compile check clean install test

all: $(BUILD_DIR) $(TARGET) test

# The binary alone. `all` also runs the suite, which needs the donor
# applications beside this tree and is not what a package builds.
# (Not named `build`: that is the object directory.)
compile: $(BUILD_DIR) $(TARGET)

# What can be checked with nothing but the binary: no donor application, no
# estate, no network.
check: compile
	sh tests/standalone_check.sh ./$(TARGET)

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(TARGET): $(OBJECTS)
	$(CC) $(CFLAGS) -Wl,-dead_strip -o $@ $^ $(LDFLAGS) $(LDLIBS)
	@echo "Built: $(TARGET)"

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c $(HEADERS) | $(BUILD_DIR)
	$(CC) $(CFLAGS) -I$(LIBBONFYRE_INC) -I../../engine/core/include -c -o $@ $<

$(BUILD_DIR)/%.o: $(LIBBONFYRE_SRC)/%.c $(HEADERS) | $(BUILD_DIR)
	$(CC) $(CFLAGS) -I$(LIBBONFYRE_INC) -I../../engine/core/include -c -o $@ $<

$(BUILD_DIR)/bf_jsonw.o $(BUILD_DIR)/bf_json.o: $(BUILD_DIR)/%.o: $(LIBBONFYRE_DIR)/%.c $(HEADERS) | $(BUILD_DIR)
	$(CC) $(CFLAGS) -I$(LIBBONFYRE_INC) -I../../engine/core/include -c -o $@ $<

# With PREFIX said, install is staged the way a package expects: into
# $(DESTDIR)$(PREFIX)/bin and nowhere else. Without it, the tree's own bin.
install: $(TARGET)
ifdef PREFIX
	install -d $(DESTDIR)$(PREFIX)/bin
	install -m 755 $(TARGET) $(DESTDIR)$(PREFIX)/bin/$(TARGET)
else
	mkdir -p $(INSTALL_DIR)
	cp $(TARGET) $(INSTALL_DIR)/
	chmod +x $(INSTALL_DIR)/$(TARGET)
	@echo "Installed to: $(INSTALL_DIR)/$(TARGET)"
endif

# Compiled conditions against the donor expressions in a real browser. Needs
# Chrome or Chromium and reports SKIP without one; kept out of `test` for that.
test-browser: $(BUILD_DIR) $(TARGET)
	bun tests/browser_condition_parity.mjs ./$(TARGET) ../../integrations/frappe-bench/apps/frappe

test: $(BUILD_DIR) $(TARGET)
	@echo "Running native compiler boundary check..."
	./$(TARGET) --help >/dev/null
	./tests/native_schema_ingest_smoke.sh ./$(TARGET)
	sh ./tests/cms_status_roundtrip_test.sh ./$(TARGET)
	./tests/surface_program_contract_test.sh ./$(TARGET)
	./tests/condition_plan_contract_test.sh ./$(TARGET)
	./tests/notification_held_message_test.sh ./$(TARGET)
	./tests/metric_read_test.sh ./$(TARGET)

clean:
	rm -rf $(BUILD_DIR) $(TARGET)
	@echo "Cleaned"
