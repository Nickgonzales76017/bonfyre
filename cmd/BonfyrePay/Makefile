CC      ?= cc
CFLAGS  ?= -O2 -Wall -Wextra -std=c11
CFLAGS  += -I../../lib/libbonfyre/include
LDFLAGS ?= ../../lib/libbonfyre/libbonfyre.a -lsqlite3
TARGET   = bonfyre-pay

all: $(TARGET)

$(TARGET): src/main.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

smoke: $(TARGET)
	$(MAKE) -C ../BonfyreMeter all
	sh tests/invoice_test.sh ./$(TARGET) ../BonfyreMeter/bonfyre-meter

clean:
	rm -f $(TARGET)
