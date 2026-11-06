CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
CFLAGS += -I../BonfyreLex/src
CURL_CFLAGS ?= $(shell pkg-config --cflags libcurl 2>/dev/null)
CURL_LIBS ?= $(shell pkg-config --libs libcurl 2>/dev/null)

TARGET := bonfyre-websub

.PHONY: all clean test native-witness
all: $(TARGET)

$(TARGET): src/main.c ../../lib/libbonfyre/include/bf_command_lookup.h
	$(CC) $(CFLAGS) -I../../lib/libbonfyre/include $(CURL_CFLAGS) -o $@ $< -lsqlite3 $(if $(CURL_LIBS),$(CURL_LIBS),-lcurl)

test: $(TARGET)
	./tests/websub_test.sh ./$(TARGET)
	$(MAKE) -C ../BonfyreLoweringWorkers
	$(MAKE) -C ../BonfyreExternalEstate bonfyre-external-estate
	./tests/material_demand_bridge_test.sh ./$(TARGET) ../BonfyreLoweringWorkers/bonfyre-lowering-workers ../BonfyreExternalEstate/bonfyre-external-estate

native-witness: test

clean:
	rm -f $(TARGET)
