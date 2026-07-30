# BonfyreFrappeCompiler Makefile

CC = clang
CFLAGS = -std=c11 -O2 -Wall -Wextra -Werror -pedantic -D_POSIX_C_SOURCE=200809L
LDFLAGS = -lsqlite3

TARGET = bonfyre-frappe-compiler
SRC_DIR = src
BUILD_DIR = build
INSTALL_DIR = ../../bin

LIBBONFYRE_DIR = ../../lib/libbonfyre
LIBBONFYRE_INC = $(LIBBONFYRE_DIR)/include
LIBBONFYRE_SRC = $(LIBBONFYRE_DIR)/src

SOURCES = $(SRC_DIR)/main.c \
          $(LIBBONFYRE_SRC)/bonfyre_frappe_compiler.c

HEADERS = $(LIBBONFYRE_INC)/bonfyre_frappe.h

OBJECTS = $(patsubst $(SRC_DIR)/%.c,$(BUILD_DIR)/%.o,$(filter $(SRC_DIR)/%.c,$(SOURCES))) \
          $(patsubst $(LIBBONFYRE_SRC)/%.c,$(BUILD_DIR)/%.o,$(filter $(LIBBONFYRE_SRC)/%.c,$(SOURCES)))

.PHONY: all clean install test

all: $(BUILD_DIR) $(TARGET)

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(TARGET): $(OBJECTS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)
	@echo "Built: $(TARGET)"

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c $(HEADERS)
	$(CC) $(CFLAGS) -I$(LIBBONFYRE_INC) -c -o $@ $<

$(BUILD_DIR)/%.o: $(LIBBONFYRE_SRC)/%.c $(HEADERS)
	$(CC) $(CFLAGS) -I$(LIBBONFYRE_INC) -c -o $@ $<

install: $(TARGET)
	mkdir -p $(INSTALL_DIR)
	cp $(TARGET) $(INSTALL_DIR)/
	chmod +x $(INSTALL_DIR)/$(TARGET)
	@echo "Installed to: $(INSTALL_DIR)/$(TARGET)"

test: $(TARGET)
	@echo "Running compiler tests..."
	sh tests/schema_ingest_smoke.sh

clean:
	rm -rf $(BUILD_DIR) $(TARGET)
	@echo "Cleaned"
