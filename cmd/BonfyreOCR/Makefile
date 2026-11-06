TARGET = bonfyre-ocr
BINDIR ?= $(HOME)/.bonfyre/bin

all: $(TARGET)

$(TARGET): src/main.swift
	swiftc -O -o $(TARGET) src/main.swift

test: $(TARGET)
	sh tests/ocr_test.sh ./$(TARGET)

install: $(TARGET)
	mkdir -p $(BINDIR) && cp $(TARGET) $(BINDIR)/$(TARGET)

clean:
	rm -f $(TARGET)

.PHONY: all test install clean
