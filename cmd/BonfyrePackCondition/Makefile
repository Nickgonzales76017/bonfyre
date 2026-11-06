CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
TARGET := bonfyre-pack-condition
OUTBOX := bonfyre-pack-condition-outbox

.PHONY: all test clean

all: test

$(TARGET): src/main.c
	$(CC) $(CFLAGS) -o $@ $< -lsqlite3

$(OUTBOX): src/outbox.c
	$(CC) $(CFLAGS) -o $@ $< -lsqlite3

test: $(TARGET) $(OUTBOX)
	cue vet condition-contract.cue
	sh tests/pack_condition_test.sh ./$(TARGET)
	sh tests/outbox_test.sh ./$(TARGET) ./$(OUTBOX)

clean:
	rm -f $(TARGET) $(OUTBOX)
