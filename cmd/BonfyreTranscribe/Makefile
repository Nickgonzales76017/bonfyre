CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
FRAGMENT_INC = ../../lib/libfragment/include
FRAGMENT_LIB = ../../lib/libfragment
OPENSSL_CFLAGS := $(shell pkg-config --cflags openssl  2>/dev/null || echo "-I/opt/homebrew/opt/openssl@3/include")
OPENSSL_LIBS   := $(shell pkg-config --libs   openssl  2>/dev/null || echo "-L/opt/homebrew/opt/openssl@3/lib -lssl -lcrypto")
SQLITE_CFLAGS  := $(shell pkg-config --cflags sqlite3  2>/dev/null || echo "")
SQLITE_LIBS    := $(shell pkg-config --libs   sqlite3  2>/dev/null || echo "-L/opt/homebrew/opt/sqlite/lib -lsqlite3")
WHISPER_INC    := $(shell pkg-config --cflags whisper  2>/dev/null || echo "-I/opt/homebrew/include")
WHISPER_LIBS   := $(shell pkg-config --libs   whisper  2>/dev/null || echo "-L/opt/homebrew/lib -lwhisper -lggml")
GGML_CFLAGS    := $(shell pkg-config --cflags ggml 2>/dev/null || echo "-I/opt/homebrew/include")
GGML_LIBS      := $(shell pkg-config --libs ggml 2>/dev/null || echo "-L/opt/homebrew/lib -lggml")
LOCAL_CFLAGS = -I../../lib/libbonfyre/include \
               -I$(FRAGMENT_INC) \
               $(OPENSSL_CFLAGS) $(SQLITE_CFLAGS) $(WHISPER_INC) $(GGML_CFLAGS)
LDFLAGS  = -L../../lib/libbonfyre -lbonfyre $(WHISPER_LIBS) $(GGML_LIBS) -lz -lm \
           -L$(FRAGMENT_LIB) -lfragment \
           $(OPENSSL_LIBS) \
           $(SQLITE_LIBS)
TARGET = bonfyre-transcribe
SRC = src/main.c

all: $(FRAGMENT_LIB)/libfragment.a $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

$(FRAGMENT_LIB)/libfragment.a:
	$(MAKE) -C $(FRAGMENT_LIB)

clean:
	rm -f $(TARGET)

.PHONY: all clean
