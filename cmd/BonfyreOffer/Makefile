CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LDFLAGS  = -L../../lib/libbonfyre -lbonfyre

all: bonfyre-offer

bonfyre-offer: src/main.c
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o bonfyre-offer src/main.c $(LDFLAGS)

clean:
	rm -f bonfyre-offer
