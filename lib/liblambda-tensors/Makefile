CC       ?= cc
AR       ?= ar
OPTFLAGS ?= -O2 -Wall -Wextra -std=c11
CFLAGS    = $(OPTFLAGS) -Iinclude
LDFLAGS  ?= -lm
UNAME_S  := $(shell uname -s)

SRC       = src/lt_core.c
OBJ       = $(SRC:.c=.o)
TARGET_A  = liblambda-tensors.a
TARGET_SO = liblambda-tensors.so
TEST_SRC  = test/test_lambda_tensors.c
TEST_BIN  = test_lambda_tensors

ifeq ($(UNAME_S),Darwin)
  # Without an absolute install name, dyld bakes the bare filename into every
  # consumer's load command and can't resolve it outside this directory.
  SO_LDFLAGS = -install_name "$(CURDIR)/$(TARGET_SO)"
else
  SO_LDFLAGS = -Wl,-soname,$(TARGET_SO)
endif

.PHONY: all clean test

all: $(TARGET_A) $(TARGET_SO)

$(TARGET_A): $(OBJ)
	$(AR) rcs $@ $^

$(TARGET_SO): $(SRC)
	$(CC) $(CFLAGS) -shared -fPIC $(SO_LDFLAGS) -o $@ $^ $(LDFLAGS)

src/lt_core.o: src/lt_core.c include/lambda_tensors.h
	$(CC) $(CFLAGS) -fPIC -c -o $@ $<

test: $(TEST_BIN)
	./$(TEST_BIN)

$(TEST_BIN): $(TEST_SRC) $(TARGET_A)
	$(CC) $(CFLAGS) -o $@ $< -L. -llambda-tensors $(LDFLAGS)

clean:
	rm -f $(OBJ) $(TARGET_A) $(TARGET_SO) $(TEST_BIN)
