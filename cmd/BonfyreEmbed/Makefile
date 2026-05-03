CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
SQLITE_CFLAGS  := $(shell pkg-config --cflags sqlite3     2>/dev/null || echo "-I/opt/homebrew/opt/sqlite/include")
SQLITE_LIBS    := $(shell pkg-config --libs   sqlite3     2>/dev/null || echo "-L/opt/homebrew/opt/sqlite/lib -lsqlite3")
# ONNX Runtime: try pkg-config, then Linux /usr paths, then Homebrew
ONNX_CFLAGS    := $(shell pkg-config --cflags onnxruntime 2>/dev/null || \
                   ([ -d /usr/include/onnxruntime ] && echo "-I/usr/include/onnxruntime") || \
                   ([ -d /usr/local/include/onnxruntime ] && echo "-I/usr/local/include/onnxruntime") || \
                   echo "-I/opt/homebrew/include/onnxruntime")
ONNX_LIBS      := $(shell pkg-config --libs   onnxruntime 2>/dev/null || echo "-lonnxruntime")
LOCAL_CFLAGS = $(ONNX_CFLAGS) $(SQLITE_CFLAGS) -I../../lib/libbonfyre/include
LDFLAGS ?= $(ONNX_LIBS) $(SQLITE_LIBS) -lm -L../../lib/libbonfyre -lbonfyre
TARGET = bonfyre-embed
SRC = src/main.c

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

clean:
	rm -f $(TARGET)

.PHONY: all clean
