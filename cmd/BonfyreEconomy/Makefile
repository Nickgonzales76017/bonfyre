CC      = cc
CFLAGS  = -std=c11 -O2 -Wall -Wextra -Wpedantic
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LDFLAGS = ../../lib/libbonfyre/libbonfyre.a -lsqlite3
TARGET  = bonfyre-economy
SRC     = src/main.c src/verification_planner.c

all: $(TARGET) verification-test

$(TARGET): $(SRC) src/verification_planner.h
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $@ $(SRC) $(LDFLAGS)

.PHONY: all verification-test
verification-test: $(TARGET)
	sh tests/verification_planner_test.sh ./$(TARGET)

clean:
	rm -f $(TARGET)
