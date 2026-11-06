CC ?= cc
CFLAGS ?= -std=c11 -O2 -Wall -Wextra -Wpedantic -Werror
TARGET = bonfyre-project-mcp
SRC = src/main.c

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $@ $(SRC) -lsqlite3

test: $(TARGET)
	./$(TARGET) self-test
	./$(TARGET) public-self-test
	./$(TARGET) ratchet
	sh tests/work_session_test.sh ./$(TARGET)

clean:
	rm -f $(TARGET)
