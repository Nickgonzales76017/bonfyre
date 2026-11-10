CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
TARGET := bonfyre-resource-weather-delta
.PHONY: all test clean
all: $(TARGET) test
$(TARGET): src/main.c ../../lib/libbonfyre/src/bf_sha256.c
	$(CC) $(CFLAGS) -I../../lib/libbonfyre/include -o $@ $< ../../lib/libbonfyre/src/bf_sha256.c -lsqlite3
test: $(TARGET)
	sh tests/resource_weather_delta_test.sh ./$(TARGET)
clean:
	rm -f $(TARGET)
