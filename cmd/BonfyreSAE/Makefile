CC      = cc
CFLAGS  = -std=c11 -O2 -Wall -Wextra -Wpedantic
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LDFLAGS = -L$(HOME)/.local/lib -lbonfyre -lsqlite3 -lm
TARGET  = bonfyre-sae
SRC     = src/main.c

$(TARGET): $(SRC)
	@mkdir -p $(HOME)/.local/lib
	@cp ../../lib/libbonfyre/libbonfyre.dylib $(HOME)/.local/lib/libbonfyre.dylib
		@install_name_tool -id $(HOME)/.local/lib/libbonfyre.dylib $(HOME)/.local/lib/libbonfyre.dylib
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $@ $(SRC) $(LDFLAGS) \
		-Wl,-rpath,$(HOME)/.local/lib

install: $(TARGET)
	install -m 755 $(TARGET) ~/.local/bin/$(TARGET)

clean:
	rm -f $(TARGET)
