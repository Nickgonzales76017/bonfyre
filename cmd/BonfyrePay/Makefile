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

test: $(TARGET)
	$(MAKE) -C ../BonfyreMeter bonfyre-meter
	sh tests/invoice_test.sh ./$(TARGET) ../BonfyreMeter/bonfyre-meter
	tmp=`mktemp -d $${TMPDIR:-/tmp}/bonfyre-pay-fixture.XXXXXX`; trap "rm -rf $$tmp" EXIT INT TERM; sh tests/make_meter_fixture.sh ../BonfyreMeter/bonfyre-meter "$$tmp/fixture.db"; sh tests/payment_test.sh ./$(TARGET) "$$tmp/fixture.db"
	cd ../.. && tests/vectors/bonfyre-vectors --root . cmd/BonfyrePay/tests/vectors/command_behavior.json

MANAGED_PREFIX ?= $(HOME)/.bonfyre
install: all
	install -d "$(MANAGED_PREFIX)/bin"
	install -m 755 $(TARGET) "$(MANAGED_PREFIX)/bin/.bonfyre-pay.next"
	mv -f "$(MANAGED_PREFIX)/bin/.bonfyre-pay.next" "$(MANAGED_PREFIX)/bin/bonfyre-pay"

clean:
	rm -f $(TARGET)

.PHONY: all clean smoke test install
