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
	@echo "==> Native Bonfyre fabric suite"
	@$(MAKE) -C ../engine/core test
