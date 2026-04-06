CC       ?= cc
AR       ?= ar
CFLAGS   ?= -O2 -Wall -Wextra -std=c11 -Iinclude
LDFLAGS  ?= -lm

SRC       = src/lt_core.c
OBJ       = $(SRC:.c=.o)
TARGET_A  = liblambda-tensors.a
TARGET_SO = liblambda-tensors.so
TEST_SRC  = test/test_lambda_tensors.c
TEST_BIN  = test_lambda_tensors

.PHONY: all clean test

all: $(TARGET_A) $(TARGET_SO)

$(TARGET_A): $(OBJ)
	$(AR) rcs $@ $^

$(TARGET_SO): $(SRC)
	$(CC) $(CFLAGS) -shared -fPIC -o $@ $^ $(LDFLAGS)

src/lt_core.o: src/lt_core.c include/lambda_tensors.h
	$(CC) $(CFLAGS) -fPIC -c -o $@ $<

test: $(TEST_BIN)
	./$(TEST_BIN)

$(TEST_BIN): $(TEST_SRC) $(TARGET_A)
	$(CC) $(CFLAGS) -o $@ $< -L. -llambda-tensors $(LDFLAGS)

clean:
	rm -f $(OBJ) $(TARGET_A) $(TARGET_SO) $(TEST_BIN)
