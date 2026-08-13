SUBDIRS = \
	BonfyreMediaPrep \
	BonfyreTranscribe \
	BonfyreTranscriptClean \
	BonfyreParagraph \
	BonfyreBrief \
	BonfyreNarrate \
	BonfyrePack \
	BonfyreQueue \
	BonfyreLedger \
	BonfyrePipeline \
	BonfyreCMS \
	BonfyreIndex \
	BonfyreStitch \
	BonfyreGate \
	BonfyreCompress \
	BonfyreEmbed \
	BonfyreWeaviateIndex \
	BonfyreCLI \
	BonfyreTranscriptFamily \
	BonfyreRender \
	BonfyreRuntime \
	BonfyreProject

.PHONY: all core fusion clean list smoke

all: core fusion

core:
	@for d in $(SUBDIRS); do \
		echo "==> $$d"; \
		$(MAKE) -C $$d || exit $$?; \
	done

fusion:
	@for d in BonfyreTranscriptFamily BonfyreRender BonfyreRuntime BonfyreProject; do \
		echo "==> $$d"; \
		$(MAKE) -C $$d || exit $$?; \
	done

clean:
	@for d in $(SUBDIRS); do \
		if [ -f $$d/Makefile ]; then \
			echo "==> clean $$d"; \
			$(MAKE) -C $$d clean || exit $$?; \
		fi; \
	done

list:
	@printf "%s\n" $(SUBDIRS)

smoke:
	@echo "==> BonfyreCMS smoke"
	@bash BonfyreCMS/tests/smoke.sh
	@echo "==> ArtifactManifest smoke"
	@bash ArtifactManifest/tests/smoke.sh
	@echo "==> LocalAITranscriptionService tests"
	@cd LocalAITranscriptionService && PYTHONPATH=src python3 -m unittest discover -s tests
	@echo "==> RNNTGrpcServer smoke"
	@bash RNNTGrpcServer/tests/smoke.sh
