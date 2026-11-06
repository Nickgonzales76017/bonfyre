CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
TARGET := bonfyre-media-delivery

.PHONY: all clean test
all: $(TARGET)
$(TARGET): src/main.c
	$(CC) $(CFLAGS) -o $@ $< -lsqlite3
test: $(TARGET)
	./tests/media_delivery_test.sh ./$(TARGET)
clean:
	rm -f $(TARGET)
