CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
SQLITE_PREFIX  := $(shell brew --prefix sqlite 2>/dev/null)
SQLITE_CFLAGS  := $(shell pkg-config --cflags sqlite3 2>/dev/null) $(if $(SQLITE_PREFIX),-I$(SQLITE_PREFIX)/include,-I/opt/homebrew/opt/sqlite/include)
SQLITE_LIBS    := $(shell pkg-config --libs sqlite3 2>/dev/null) $(if $(SQLITE_PREFIX),-L$(SQLITE_PREFIX)/lib,-L/opt/homebrew/opt/sqlite/lib) -lsqlite3
# ONNX Runtime: try pkg-config, then Linux /usr paths, then Homebrew
ONNX_PREFIX    := $(shell brew --prefix onnxruntime 2>/dev/null)
ONNX_CFLAGS    := $(shell pkg-config --cflags onnxruntime 2>/dev/null) $(if $(ONNX_PREFIX),-I$(ONNX_PREFIX)/include/onnxruntime,-I/opt/homebrew/include/onnxruntime)
ONNX_LIBS      := $(shell pkg-config --libs onnxruntime 2>/dev/null) $(if $(ONNX_PREFIX),-L$(ONNX_PREFIX)/lib) -lonnxruntime
LOCAL_CFLAGS = $(ONNX_CFLAGS) $(SQLITE_CFLAGS) -DSQLITE_ENABLE_LOAD_EXTENSION -I../../lib/libbonfyre/include
LDFLAGS ?= $(ONNX_LIBS) $(SQLITE_LIBS) -lm -L../../lib/libbonfyre -lbonfyre
TARGET = bonfyre-embed
SRC = src/main.c

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

clean:
	rm -f $(TARGET)

.PHONY: all clean
