CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
TARGET := bonfyre-scoped-respecialize

.PHONY: all test clean

all: $(TARGET) test

$(TARGET): src/main.c
	$(CC) $(CFLAGS) -o $@ $< -lsqlite3

test: $(TARGET)
	$(MAKE) -C ../BonfyreScopedDemand bonfyre-scoped-demand
	$(MAKE) -C ../BonfyreScopedFrontier bonfyre-scoped-frontier
	$(MAKE) -C ../BonfyreLoweringWorkers bonfyre-lowering-workers
	$(MAKE) -C ../BonfyreWebSub bonfyre-websub
	sh tests/scoped_respecialize_test.sh ./$(TARGET) ../BonfyreScopedDemand/bonfyre-scoped-demand ../BonfyreScopedFrontier/bonfyre-scoped-frontier
	sh tests/scoped_respecialize_scale_test.sh ./$(TARGET) ../BonfyreScopedDemand/bonfyre-scoped-demand ../BonfyreScopedFrontier/bonfyre-scoped-frontier
	sh tests/scoped_observation_loop_test.sh ./$(TARGET) ../BonfyreScopedDemand/bonfyre-scoped-demand ../BonfyreScopedFrontier/bonfyre-scoped-frontier ../BonfyreLoweringWorkers/bonfyre-lowering-workers ../BonfyreWebSub/bonfyre-websub

clean:
	rm -f $(TARGET)
