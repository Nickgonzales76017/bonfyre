CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
TARGET = bonfyre-world-compiler
JSON = generated/world-fragment-lowering.json
HEADER = generated/native_world_fragment_lowerers.h
SRC = src/main.c

.PHONY: all clean

all: $(TARGET)

$(JSON): world-fragment-lowering.cue
	mkdir -p generated
	cue export $< -e nativeWorldFragmentLowerers --out json > $@

$(HEADER): $(JSON) tools/compile_world_fragment_lowerers.zig
	zig run tools/compile_world_fragment_lowerers.zig -- $(JSON) $(HEADER)

$(TARGET): $(SRC) $(HEADER)
	$(CC) $(CFLAGS) -Igenerated -o $@ $(SRC) -lsqlite3

clean:
	rm -rf $(TARGET) generated
