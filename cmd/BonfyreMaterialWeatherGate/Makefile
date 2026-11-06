CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
TARGET := bonfyre-material-weather-gate

.PHONY: all test clean
all: $(TARGET) test

$(TARGET): src/main.c
	$(CC) $(CFLAGS) -o $@ $< -lsqlite3

test: $(TARGET)
	$(MAKE) -C ../BonfyreMaterialLease bonfyre-material-lease
	sh tests/material_weather_gate_test.sh ./$(TARGET) ../BonfyreMaterialLease/bonfyre-material-lease

clean:
	rm -f $(TARGET)
