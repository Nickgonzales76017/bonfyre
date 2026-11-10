CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
TARGET := bonfyre-scoped-demand
REHEAT := bonfyre-scoped-demand-reheat
CONDITION := ../BonfyrePackCondition/bonfyre-pack-condition

.PHONY: all test clean

all: test

$(TARGET): src/main.c
	$(CC) $(CFLAGS) -o $@ $< -lsqlite3

$(REHEAT): src/reheat.c
	$(CC) $(CFLAGS) -o $@ $< -lsqlite3

$(CONDITION):
	$(MAKE) -C ../BonfyrePackCondition bonfyre-pack-condition

test: $(TARGET) $(REHEAT) $(CONDITION)
	$(MAKE) -C ../BonfyreLoweringWorkers
	$(MAKE) -C ../BonfyreExternalEstate bonfyre-external-estate
	sh tests/scoped_demand_test.sh ./$(TARGET) ../BonfyreLoweringWorkers/bonfyre-lowering-workers
	sh tests/condition_gate_test.sh ./$(TARGET) $(CONDITION) ../BonfyreExternalEstate/bonfyre-external-estate
	sh tests/reheat_test.sh ./$(TARGET) ./$(REHEAT) ../BonfyreExternalEstate/bonfyre-external-estate ../BonfyreLoweringWorkers/bonfyre-lowering-workers

clean:
	rm -f $(TARGET) $(REHEAT)
