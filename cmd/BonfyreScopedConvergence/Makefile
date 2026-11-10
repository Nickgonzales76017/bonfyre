.PHONY: all test deps

all: test

deps:
	$(MAKE) -C ../BonfyrePackCondition bonfyre-pack-condition bonfyre-pack-condition-outbox
	$(MAKE) -C ../BonfyrePackConditionDelta bonfyre-pack-condition-delta
	$(MAKE) -C ../BonfyreScopedDemand bonfyre-scoped-demand
	$(MAKE) -C ../BonfyreScopedFrontier bonfyre-scoped-frontier
	$(MAKE) -C ../BonfyreMaterialLease bonfyre-material-lease
	$(MAKE) -C ../BonfyreScopedLeaseReheat bonfyre-scoped-lease-reheat

test: deps
	sh tests/condition_lease_convergence_test.sh \
	  ../BonfyrePackCondition/bonfyre-pack-condition ../BonfyrePackCondition/bonfyre-pack-condition-outbox \
	  ../BonfyrePackConditionDelta/bonfyre-pack-condition-delta ../BonfyreScopedDemand/bonfyre-scoped-demand \
	  ../BonfyreScopedFrontier/bonfyre-scoped-frontier ../BonfyreMaterialLease/bonfyre-material-lease \
	  ../BonfyreScopedLeaseReheat/bonfyre-scoped-lease-reheat
