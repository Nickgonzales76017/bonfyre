.PHONY: all test clean

SCOPED := ../BonfyreScopedDemand/bonfyre-scoped-demand
WORKERS := ../BonfyreLoweringWorkers/bonfyre-lowering-workers
WEBSUB := ../BonfyreWebSub/bonfyre-websub
ESTATE := ../BonfyreExternalEstate/bonfyre-external-estate
WEB := ../BonfyreWebAddress/bonfyre-web-address

all: test

$(SCOPED):
	$(MAKE) -C ../BonfyreScopedDemand bonfyre-scoped-demand
$(WORKERS):
	$(MAKE) -C ../BonfyreLoweringWorkers bonfyre-lowering-workers
$(WEBSUB):
	$(MAKE) -C ../BonfyreWebSub bonfyre-websub
$(ESTATE):
	$(MAKE) -C ../BonfyreExternalEstate bonfyre-external-estate
$(WEB):
	$(MAKE) -C ../BonfyreWebAddress bonfyre-web-address

test: $(SCOPED) $(WORKERS) $(WEBSUB) $(ESTATE) $(WEB)
	sh tests/world_reheat_loop_test.sh $(SCOPED) $(WORKERS) $(WEBSUB) $(ESTATE) $(WEB)

clean:
