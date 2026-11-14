CC      ?= cc
CFLAGS  ?= -O2 -Wall -Wextra -std=c11
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LDFLAGS      = -L../../lib/libbonfyre -lbonfyre -lm
TARGET       = bonfyre-sli
SRC          = src/main.c

.PHONY: all clean test offline-test model-contract-test

all: $(TARGET) offline-test

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $@ $^ $(LDFLAGS)

offline-test: $(TARGET)
	sh ../BonfyreQuant/tests/bqfp_offline_test.sh sli ./$(TARGET)

# Requires BONFYRE_TEST_MODEL with a real .safetensors model.
model-contract-test: $(TARGET)
	sh tests/bqfp_corruption_test.sh ./$(TARGET)

test: offline-test model-contract-test

clean:
	rm -f $(TARGET)
