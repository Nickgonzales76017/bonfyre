# bonfyre-arrow-stream: Parquet in, Arrow IPC stream out.
#
# The Arrow C++ SDK comes from a package (Homebrew or MacPorts apache-arrow),
# found through pkg-config. It is never taken from a Python wheel: a wheel's
# bundled libraries are that wheel's private files, not an SDK.
CXX ?= c++
PKG_CONFIG ?= pkg-config
TARGET = bonfyre-arrow-stream
# The SDK's headers are another project's code: included as system headers,
# so this tree's warnings-as-errors judge this tree's source and not theirs.
ARROW_CFLAGS := $(patsubst -I%,-isystem %,$(shell $(PKG_CONFIG) --cflags arrow parquet 2>/dev/null))
ARROW_LIBS := $(shell $(PKG_CONFIG) --libs arrow parquet 2>/dev/null)
ARROW_LIBDIR := $(shell $(PKG_CONFIG) --variable=libdir arrow 2>/dev/null)
CXXFLAGS ?= -O2
WERROR ?= -Werror

.PHONY: all test install clean

all: $(TARGET)

$(TARGET): src/main.cpp
	@[ -n "$(ARROW_LIBS)" ] || { echo "no packaged Arrow C++ SDK found by $(PKG_CONFIG) (install apache-arrow)" >&2; exit 1; }
	$(CXX) -std=c++20 -Wall -Wextra $(WERROR) $(CXXFLAGS) $(CPPFLAGS) $(ARROW_CFLAGS) src/main.cpp $(LDFLAGS) $(ARROW_LIBS) -o $@

# The existing witness, handed the same packaged SDK the binary was built with.
test: $(TARGET)
	ARROW_CPP_INCLUDE="$$($(PKG_CONFIG) --variable=includedir arrow)" \
	ARROW_CPP_LIBARROW="$(ARROW_LIBDIR)/libarrow.dylib" \
	ARROW_CPP_LIBPARQUET="$(ARROW_LIBDIR)/libparquet.dylib" \
	sh tests/arrow_stream_test.sh

install: $(TARGET)
	@[ -n "$(PREFIX)" ] || { echo "say PREFIX (and DESTDIR for a staged install)" >&2; exit 1; }
	install -d $(DESTDIR)$(PREFIX)/bin
	install -m 755 $(TARGET) $(DESTDIR)$(PREFIX)/bin/$(TARGET)

clean:
	rm -f $(TARGET)
