CC      = cc
CFLAGS  = -std=c11 -O2 -Wall -Wextra -Wpedantic
LOCAL_CFLAGS = -I../../lib/libbonfyre/include
LDFLAGS = -L../../lib/libbonfyre -lbonfyre -lsqlite3
TARGET  = bonfyre-learn
SRC     = src/main.c

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) -o $@ $(SRC) $(LDFLAGS)

.PHONY: test clean
test: $(TARGET)
	@set -eu; witness=$$(mktemp "$${TMPDIR:-/tmp}/bonfyre-learn-stage.XXXXXX"); \
	trap 'rm -f "$$witness"' 0 1 2 15; \
	$(CC) $(CFLAGS) $(LOCAL_CFLAGS) tests/feedback_stage_test.c -o "$$witness" $(LDFLAGS); "$$witness"

clean:
	rm -f $(TARGET)
