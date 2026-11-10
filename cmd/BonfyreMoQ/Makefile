CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
LOCAL_CFLAGS = -I../../lib/libbonfyre/include -I../../lib/libquic-transport/include
QUIC_CFLAGS ?= $(shell pkg-config --cflags libngtcp2 libngtcp2_crypto_ossl 2>/dev/null)
QUIC_LIBS ?= $(shell pkg-config --libs libngtcp2 libngtcp2_crypto_ossl openssl 2>/dev/null)
LDFLAGS = ../../lib/libbonfyre/libbonfyre.a ../../lib/libquic-transport/libquic-transport.a -lsqlite3 -lm $(QUIC_LIBS)
TARGET = bonfyre-moq
SRC = src/main.c

all: $(TARGET)

test: $(TARGET)
	./tests/moqt_wire_test.sh ./$(TARGET)
	./tests/moqt_native_session_test.sh ./$(TARGET)
	./tests/moqt_leased_session_test.sh ./$(TARGET)

$(TARGET): $(SRC) ../../lib/libquic-transport/libquic-transport.a
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) $(QUIC_CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

../../lib/libquic-transport/libquic-transport.a:
	$(MAKE) -C ../../lib/libquic-transport all

clean:
	rm -f $(TARGET)

install: $(TARGET)
	install -D $(TARGET) $(DESTDIR)$(PREFIX)/bin/$(TARGET)

.PHONY: all clean install test
