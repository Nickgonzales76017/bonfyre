CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LDFLAGS  = -L../../lib/libbonfyre -lbonfyre -lm
TARGET = bonfyre-quant
SRC = src/main.c

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

# Tests measure a real model the caller names; nothing synthetic is measured.
# Example: BONFYRE_QUANT_MODEL=/path/to/model.safetensors
#          BONFYRE_QUANT_TENSOR=model.layers.0.input_layernorm.weight
SLI ?= ../BonfyreSLI/bonfyre-sli
FPQX ?= ../BonfyreFPQx/bonfyre-fpqx

ifeq ($(strip $(BONFYRE_QUANT_MODEL))$(strip $(BONFYRE_QUANT_TENSOR)),)
test:
	@echo "test: set BONFYRE_QUANT_MODEL and BONFYRE_QUANT_TENSOR to a real model and tensor; skipping" >&2
else
test: $(TARGET)
	sh tests/roundtrip_test.sh ./$(TARGET) "$(BONFYRE_QUANT_MODEL)"
	sh tests/bqfp_malformed_test.sh ./$(TARGET) $(SLI) $(FPQX) "$(BONFYRE_QUANT_MODEL)"
	sh tests/bqfp_roundtrip_test.sh ./$(TARGET) $(SLI) $(FPQX) "$(BONFYRE_QUANT_MODEL)"
	python3 tests/physical_roundtrip_test.py ./$(TARGET) $(SLI) "$(BONFYRE_QUANT_MODEL)" "$(BONFYRE_QUANT_TENSOR)"
endif

MANAGED_PREFIX ?= $(HOME)/.bonfyre
install: all
	install -d "$(MANAGED_PREFIX)/bin"
	install -m 755 $(TARGET) "$(MANAGED_PREFIX)/bin/.bonfyre-quant.next"
	mv -f "$(MANAGED_PREFIX)/bin/.bonfyre-quant.next" "$(MANAGED_PREFIX)/bin/bonfyre-quant"

clean:
	rm -f $(TARGET)

.PHONY: all clean test install
