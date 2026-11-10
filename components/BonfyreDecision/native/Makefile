CC ?= cc
ROOT := ../../..
NODE_INCLUDE ?= $(shell node -p 'require("path").resolve(process.execPath,"../../include/node")')
BUILD ?= build
CFLAGS ?= -O2 -std=c11 -Wall -Wextra -Werror
CPPFLAGS += -I$(ROOT)/lib/libbonfyre/include -I$(NODE_INCLUDE) -DNAPI_VERSION=8
LDFLAGS += -lm
ifeq ($(shell uname -s),Darwin)
  LDFLAGS += -undefined dynamic_lookup
  ifneq ($(SCALAR),1)
    CPPFLAGS += -DBF_DECISION_ACCELERATE -DACCELERATE_NEW_LAPACK
    LDFLAGS += -framework Accelerate
  endif
endif
SOURCES := head_binding.c $(ROOT)/lib/libbonfyre/src/bf_decision_head.c
.PHONY: all
all: $(BUILD)/decision_head.node
$(BUILD)/decision_head.node: $(SOURCES) $(ROOT)/lib/libbonfyre/include/bf_decision_head.h
	mkdir -p $(BUILD)
	$(CC) $(CFLAGS) $(CPPFLAGS) -shared -fPIC $(SOURCES) $(LDFLAGS) -o $@

.PHONY: sanitize
sanitize:
	mkdir -p $(BUILD)
	$(CC) -O1 -g -std=c11 -Wall -Wextra -Werror -fsanitize=address,undefined -fno-omit-frame-pointer -I$(ROOT)/lib/libbonfyre/include ../tests/native-head-safety.c $(ROOT)/lib/libbonfyre/src/bf_decision_head.c -lm -o $(BUILD)/head-safety
	$(BUILD)/head-safety

# Current guarded raw ABI versus the immutable owner. Historical pre-change
# measurements and source/binary fingerprints are retained in the JSON witness.
.PHONY: benchmark
benchmark:
	mkdir -p $(BUILD)
	$(CC) $(CFLAGS) $(CPPFLAGS) ../tests/native-head-bench.c $(ROOT)/lib/libbonfyre/src/bf_decision_head.c $(LDFLAGS) -o $(BUILD)/head-bench-raw
	$(CC) $(CFLAGS) $(CPPFLAGS) -DBF_DECISION_BENCH_OWNED ../tests/native-head-bench.c $(ROOT)/lib/libbonfyre/src/bf_decision_head.c $(LDFLAGS) -o $(BUILD)/head-bench-owned
	VECLIB_MAXIMUM_THREADS=1 $(BUILD)/head-bench-raw
	VECLIB_MAXIMUM_THREADS=1 $(BUILD)/head-bench-owned
