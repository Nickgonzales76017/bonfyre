CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LDFLAGS  = -L../../lib/libbonfyre -lbonfyre

all: bonfyre-pack

bonfyre-pack: src/main.c
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o bonfyre-pack src/main.c $(LDFLAGS)

clean:
	rm -f bonfyre-pack
