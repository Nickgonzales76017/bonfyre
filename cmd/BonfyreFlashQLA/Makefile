CC      ?= cc
CFLAGS  ?= -O2 -Wall -Wextra -std=c11
LOCAL_CFLAGS = -I../../lib/libbonfyre/include -Isrc
LDFLAGS      = -L../../lib/libbonfyre -lbonfyre -lm
TARGET       = bonfyre-flashqla
SRC          = src/main.c src/gdn_ref.c src/gdn_tile.c

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $@ $^ $(LDFLAGS)

clean:
	rm -f $(TARGET)
