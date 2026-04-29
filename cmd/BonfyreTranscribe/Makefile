CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
FRAGMENT_INC = ../../lib/libfragment/include
FRAGMENT_LIB = ../../lib/libfragment
OSSL_INC     = /opt/homebrew/opt/openssl@3/include
OSSL_LIB     = /opt/homebrew/opt/openssl@3/lib
SQLITE_LIB   = /opt/homebrew/opt/sqlite/lib
LOCAL_CFLAGS = -I../../lib/libbonfyre/include -I/opt/homebrew/include \
               -I$(FRAGMENT_INC) -I$(OSSL_INC)
LDFLAGS  = -L../../lib/libbonfyre -lbonfyre -L/opt/homebrew/lib -lwhisper -lggml -lz -lm \
           -L$(FRAGMENT_LIB) -lfragment \
           -L$(OSSL_LIB) -lssl -lcrypto \
           -L$(SQLITE_LIB) -lsqlite3
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
