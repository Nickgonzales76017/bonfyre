comma := ,
CC ?= cc
# -Os with dead stripping keeps the installed organ inside the native ratchet's
# installed_bonfyre_bytes budget after option, persistence and trace gates.
CFLAGS ?= -Os -Wall -Wextra -std=c11
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
DEAD_STRIP := $(if $(filter Darwin,$(shell uname -s)),-Wl$(comma)-dead_strip,-Wl$(comma)--gc-sections)
LDFLAGS  = $(DEAD_STRIP) -L../../lib/libbonfyre -lbonfyre -lm
TARGET = bonfyre-physics
SRC = src/main.c

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

test: $(TARGET)
	sh tests/bvh_output_test.sh ./$(TARGET)
	sh tests/geometry_admission_test.sh ./$(TARGET)
	sh tests/option_persistence_test.sh ./$(TARGET)
	sh tests/real_vector_witness_test.sh ./$(TARGET)

clean:
	rm -f $(TARGET)

.PHONY: all clean test
