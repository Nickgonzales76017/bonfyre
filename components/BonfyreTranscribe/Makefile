CC     ?= cc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
TARGET  = bonfyre-transcribe
SRC     = src/main.c

# whisper.cpp + ggml (Homebrew)
WHISPER_INC ?= /opt/homebrew/include
WHISPER_LIB ?= /opt/homebrew/lib
LIBS = -lwhisper -lggml -lz -lm

all: $(TARGET)

$(TARGET): $(SRC) src/hcp_subword_freq.h
	$(CC) $(CFLAGS) -I$(WHISPER_INC) -L$(WHISPER_LIB) -o $(TARGET) $(SRC) $(LIBS)

test: $(TARGET)
	sh tests/redecode_test.sh ./$(TARGET)

clean:
	rm -f $(TARGET)

.PHONY: all test clean
