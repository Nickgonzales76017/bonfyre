CC ?= cc
CFLAGS ?= -std=c11 -O2 -Wall -Wextra -Wpedantic -Werror
TARGET = bonfyre-project-mcp
SRC = src/main.c

all: test

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $@ $(SRC) -lsqlite3

retirement-proof: $(TARGET)
	./$(TARGET) self-test
	./$(TARGET) public-self-test
	cd ../.. && bun test integrations/mcp/tests/project_operator.test.mjs integrations/mcp/tests/operator_tools.test.mjs
	sh tests/work_session_test.sh ./$(TARGET)

test: retirement-proof
	./$(TARGET) ratchet

clean:
	rm -f $(TARGET)
