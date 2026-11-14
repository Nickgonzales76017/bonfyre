CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LDFLAGS = -L../../lib/libbonfyre -lbonfyre -lm
TARGET = bonfyre-fpqx
SRC = src/main.c

all: $(TARGET) offline-test

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

offline-test: $(TARGET)
	sh ../BonfyreQuant/tests/bqfp_offline_test.sh fpqx ./$(TARGET)

# Requires BONFYRE_TEST_MODEL with a real .safetensors model.
model-contract-test: $(TARGET)
	sh tests/bqfp_contract_test.sh ./$(TARGET)

test: offline-test model-contract-test

clean:
	rm -f $(TARGET)

.PHONY: all clean test offline-test model-contract-test
