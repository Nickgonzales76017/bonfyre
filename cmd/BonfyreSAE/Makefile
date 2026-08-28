CC      ?= cc
CFLAGS  ?= -std=c11 -O2 -Wall -Wextra -Wpedantic
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LDFLAGS = -L../../lib/libbonfyre -lbonfyre -lsqlite3 -lm -lpthread
TARGET  = bonfyre-sae
SRC     = src/main.c

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $@ $(SRC) $(LDFLAGS)

install: $(TARGET)
	install -m 755 $(TARGET) ~/.local/bin/$(TARGET)

clean:
	rm -f $(TARGET)
