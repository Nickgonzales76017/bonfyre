CC ?= cc
CFLAGS ?= -O2 -Wall -Wextra -Werror -std=c11
TARGET = bonfyre-interaction-grammar

.PHONY: all test clean

all: $(TARGET)

$(TARGET): src/main.c
	$(CC) $(CFLAGS) -o $@ $< -lsqlite3

test: $(TARGET) ../BonfyreExternalEstate/bonfyre-external-estate ../BonfyreLoweringWorkers/bonfyre-lowering-workers ../BonfyreChannel/bonfyre-channel ../BonfyreMediaLibrary/bonfyre-media-library
	./tests/interaction_grammar_test.sh ./$(TARGET)

../BonfyreExternalEstate/bonfyre-external-estate: ../BonfyreExternalEstate/src/main.c
	$(MAKE) -C ../BonfyreExternalEstate all

../BonfyreLoweringWorkers/bonfyre-lowering-workers: ../BonfyreLoweringWorkers/src/main.c
	$(MAKE) -C ../BonfyreLoweringWorkers all

../BonfyreChannel/bonfyre-channel: ../BonfyreChannel/src/main.c
	$(MAKE) -C ../BonfyreChannel all

../BonfyreMediaLibrary/bonfyre-media-library: ../BonfyreMediaLibrary/src/main.c
	$(MAKE) -C ../BonfyreMediaLibrary all

clean:
	rm -f $(TARGET)
