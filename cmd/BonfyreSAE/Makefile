CC      = cc
CFLAGS  = -std=c11 -O2 -Wall -Wextra -Wpedantic
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
# Link the static archive the way every other organ does.
#
# This recipe previously copied libbonfyre.dylib into $(HOME)/.local/lib and ran
# install_name_tool on it. Both are macOS-only: there is no .dylib on Linux and
# no install_name_tool, so the build could never succeed on the deploy host.
# Linking $(LIB) also removes the runtime dependency on a file under $HOME,
# which a service running with ProtectHome=true cannot read anyway.
LIB     = ../../lib/libbonfyre/libbonfyre.a
LDFLAGS = -lsqlite3 -lm
TARGET  = bonfyre-sae
SRC     = src/main.c

$(TARGET): $(SRC) $(LIB)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $@ $(SRC) $(LIB) $(LDFLAGS)

install: $(TARGET)
	install -m 755 $(TARGET) ~/.local/bin/$(TARGET)

clean:
	rm -f $(TARGET)
