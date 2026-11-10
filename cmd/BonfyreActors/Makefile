CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
CFLAGS += -I../../engine/core/include
TARGET = bonfyre-actors
SRC = src/main.c ../../engine/core/src/actor.c ../../engine/core/src/corroboration.c

all: $(TARGET)

test: $(TARGET)
	$(MAKE) -C ../BonfyreOrigin
	$(MAKE) -C ../BonfyreAgentContract
	$(MAKE) -C ../BonfyreStanding
	$(MAKE) -C ../BonfyreHousehold
	sh tests/organ_callers_test.sh

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC) -lsqlite3

clean:
	rm -f $(TARGET)
