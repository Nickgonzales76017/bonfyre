CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
ROOT := ../..
TARGET = bonfyre-repository-fabric

all: $(TARGET)

$(TARGET): src/main.c $(ROOT)/lib/libbonfyre/bf_json.c $(ROOT)/lib/libbonfyre/bf_jsonw.c $(ROOT)/lib/libbonfyre/src/bf_sha256.c
	$(CC) $(CFLAGS) -I$(ROOT)/lib/libbonfyre/include -o $@ src/main.c $(ROOT)/lib/libbonfyre/bf_json.c $(ROOT)/lib/libbonfyre/bf_jsonw.c $(ROOT)/lib/libbonfyre/src/bf_sha256.c

test: $(TARGET)
	bash ./tests/smoke.sh ./$(TARGET)

clean:
	rm -f $(TARGET)

.PHONY: all test clean
