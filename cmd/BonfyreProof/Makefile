CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LDFLAGS  = -L../../lib/libbonfyre -lbonfyre

all: bonfyre-proof

bonfyre-proof: src/main.c
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o bonfyre-proof src/main.c $(LDFLAGS)

clean:
	rm -f bonfyre-proof
