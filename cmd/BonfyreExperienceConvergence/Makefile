.PHONY: all test native-tools lowering-tools protocol-lowering live-source maybe-live-source

all: test protocol-lowering maybe-live-source

native-tools:
	$(MAKE) -C ../BonfyreEconomy bonfyre-economy
	$(MAKE) -C ../BonfyreExperienceBinding
	$(MAKE) -C ../BonfyreParticipation
	$(MAKE) -C ../BonfyreDisturbanceAudit
	$(MAKE) -C ../BonfyreObservationBoundary
	$(MAKE) -C ../BonfyreBridgeReadiness
	$(MAKE) -C ../BonfyreExperienceStage

test: native-tools
	sh tests/convergence_test.sh \
		../BonfyreExperienceBinding/bonfyre-experience-binding \
		../BonfyreParticipation/bonfyre-participation \
		../BonfyreDisturbanceAudit/bonfyre-disturbance-audit \
		../BonfyreObservationBoundary/bonfyre-observation-boundary \
		../BonfyreBridgeReadiness/bonfyre-bridge-readiness \
		../BonfyreExperienceStage/bonfyre-experience-stage
	bun test tests/reviewed_source_frontier.test.mjs

lowering-tools:
	$(MAKE) -C ../BonfyreExternalEstate bonfyre-external-estate
	$(MAKE) -C ../BonfyreLoweringWorkers bonfyre-lowering-workers

protocol-lowering: lowering-tools
	sh tests/protocol_lowering_test.sh \
		../BonfyreExternalEstate/bonfyre-external-estate ../BonfyreLoweringWorkers/bonfyre-lowering-workers \
		../BonfyreExperienceMap/generated/protocols.jsonl fixtures/protocol-x402-fact-delta.json

live-source: lowering-tools
	sh tests/live_cms_source_lowering_test.sh \
		../BonfyreExternalEstate/bonfyre-external-estate ../BonfyreLoweringWorkers/bonfyre-lowering-workers \
		$(HOME)/.bonfyre/estate/publications/corpus.db fixtures/cms-source-fact-delta.json

maybe-live-source:
	@if [ -f "$(HOME)/.bonfyre/estate/publications/corpus.db" ]; then \
		$(MAKE) live-source; \
	else \
		echo "live CMS source lowering: SKIP (no local publication corpus)"; \
	fi
