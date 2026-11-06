CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11 -ffunction-sections -fdata-sections
LOCAL_CFLAGS = -I../../engine/core/include
LDFLAGS ?= ../../engine/core/libbonfyre-fabric.a -lsqlite3
TARGET = bonfyre-lifecycle

all: $(TARGET)

$(TARGET): src/main.c ../../engine/core/libbonfyre-fabric.a
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -Wl,-dead_strip -o $@ src/main.c $(LDFLAGS)

../../engine/core/libbonfyre-fabric.a:
	$(MAKE) -C ../../engine/core

test: $(TARGET)
	./$(TARGET) lifecycle register-capability-fulfillment

clean:
	rm -f $(TARGET)
