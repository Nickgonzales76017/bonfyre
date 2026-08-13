ROOT_DIR := $(abspath $(CURDIR)/..)
AUREKAI_VERSION ?= 0.8.0-alpha.1
LEGACY_RUNTIME_VERSION ?= 0.7.0
HYPER_TARGET ?= bun-darwin-arm64
MODEL_MEMORY_FAMILY ?= qwen3-8b
DATE_TAG ?= $(shell date +%Y%m%d)

PARENT_HYPER := $(ROOT_DIR)/dist/bonfyre-hyper-v$(LEGACY_RUNTIME_VERSION)-$(HYPER_TARGET)
PARENT_RUNTIME := $(ROOT_DIR)/dist/bonfyre-runtime-v$(LEGACY_RUNTIME_VERSION)-$(HYPER_TARGET).tar.gz
PARENT_RUNTIME_ZST := $(ROOT_DIR)/dist/bonfyre-runtime-v$(LEGACY_RUNTIME_VERSION)-$(HYPER_TARGET).tar.zst
PARENT_MODEL := $(ROOT_DIR)/dist/bonfyre-model-memory-$(MODEL_MEMORY_FAMILY)-$(DATE_TAG).tar.gz
PARENT_MODEL_ZST := $(ROOT_DIR)/dist/bonfyre-model-memory-$(MODEL_MEMORY_FAMILY)-$(DATE_TAG).tar.zst
PARENT_APPLIANCE := $(ROOT_DIR)/dist/bonfyre-appliance-v$(LEGACY_RUNTIME_VERSION)-$(HYPER_TARGET).tar.gz
PARENT_APPLIANCE_ZST := $(ROOT_DIR)/dist/bonfyre-appliance-v$(LEGACY_RUNTIME_VERSION)-$(HYPER_TARGET).tar.zst

AUREKAI_DIST := $(CURDIR)/dist
AUREKAI_HYPER := $(AUREKAI_DIST)/akai-hyper-v$(AUREKAI_VERSION)-$(HYPER_TARGET)
AUREKAI_RUNTIME := $(AUREKAI_DIST)/aurekai-runtime-v$(AUREKAI_VERSION)-$(HYPER_TARGET).tar.gz
AUREKAI_RUNTIME_ZST := $(AUREKAI_DIST)/aurekai-runtime-v$(AUREKAI_VERSION)-$(HYPER_TARGET).tar.zst
AUREKAI_MODEL := $(AUREKAI_DIST)/aurekai-model-memory-$(MODEL_MEMORY_FAMILY)-$(DATE_TAG).tar.gz
AUREKAI_MODEL_ZST := $(AUREKAI_DIST)/aurekai-model-memory-$(MODEL_MEMORY_FAMILY)-$(DATE_TAG).tar.zst
AUREKAI_APPLIANCE := $(AUREKAI_DIST)/aurekai-appliance-v$(AUREKAI_VERSION)-$(HYPER_TARGET).tar.gz
AUREKAI_APPLIANCE_ZST := $(AUREKAI_DIST)/aurekai-appliance-v$(AUREKAI_VERSION)-$(HYPER_TARGET).tar.zst
AUREKAI_MANIFEST := $(AUREKAI_DIST)/aurekai.manifest.json
LEGACY_COMPAT_MANIFEST := $(AUREKAI_DIST)/bonfyre.manifest.json
AUREKAI_SHA256SUMS := $(AUREKAI_DIST)/SHA256SUMS
AUREKAI_SBOM := $(AUREKAI_DIST)/SBOM.spdx.json

.PHONY: help manifest release release-check sha256sums sbom clean

help:
	@echo "Aurekai public release wrapper"
	@echo ""
	@echo "  make manifest                      Render aurekai.manifest.json into dist/"
	@echo "  make release HYPER_TARGET=bun-darwin-arm64"
	@echo "  make release-check HYPER_TARGET=bun-darwin-arm64"
	@echo "  make sha256sums                    Generate SHA256SUMS for all dist/ artifacts"
	@echo "  make sbom                          Generate SBOM.spdx.json for current release"

manifest:
	@node ./src/render-manifest.mjs $(AUREKAI_MANIFEST)
	@echo "  -> $(AUREKAI_MANIFEST)"

release: manifest
	@mkdir -p $(AUREKAI_DIST)
	@$(MAKE) -C $(ROOT_DIR) \
		BONFYRE_VERSION=$(LEGACY_RUNTIME_VERSION) \
		HYPER_TARGET=$(HYPER_TARGET) \
		MODEL_MEMORY_FAMILY=$(MODEL_MEMORY_FAMILY) \
		appliance-package
	@cp $(PARENT_HYPER) $(AUREKAI_HYPER)
	@cp $(PARENT_RUNTIME) $(AUREKAI_RUNTIME)
	@if [ -f $(PARENT_RUNTIME_ZST) ]; then cp $(PARENT_RUNTIME_ZST) $(AUREKAI_RUNTIME_ZST); fi
	@cp $(PARENT_MODEL) $(AUREKAI_MODEL)
	@if [ -f $(PARENT_MODEL_ZST) ]; then cp $(PARENT_MODEL_ZST) $(AUREKAI_MODEL_ZST); fi
	@cp $(PARENT_APPLIANCE) $(AUREKAI_APPLIANCE)
	@if [ -f $(PARENT_APPLIANCE_ZST) ]; then cp $(PARENT_APPLIANCE_ZST) $(AUREKAI_APPLIANCE_ZST); fi
	@cp $(ROOT_DIR)/dist/bonfyre.manifest.json $(LEGACY_COMPAT_MANIFEST)
	@echo "  -> $(AUREKAI_HYPER)"
	@echo "  -> $(AUREKAI_RUNTIME)"
	@if [ -f $(AUREKAI_RUNTIME_ZST) ]; then echo "  -> $(AUREKAI_RUNTIME_ZST)"; fi
	@echo "  -> $(AUREKAI_MODEL)"
	@if [ -f $(AUREKAI_MODEL_ZST) ]; then echo "  -> $(AUREKAI_MODEL_ZST)"; fi
	@echo "  -> $(AUREKAI_APPLIANCE)"
	@if [ -f $(AUREKAI_APPLIANCE_ZST) ]; then echo "  -> $(AUREKAI_APPLIANCE_ZST)"; fi
	@echo "  -> $(AUREKAI_MANIFEST)"
	@echo "  -> $(LEGACY_COMPAT_MANIFEST)"
	@echo ""
	@echo "  NOTE: Every Aurekai release includes both manifests."
	@echo "        aurekai.manifest.json   -- public distribution metadata"
	@echo "        bonfyre.manifest.json   -- legacy runtime validation (the bridge)"

sha256sums: release
	@cd $(AUREKAI_DIST) && \
	  find . -maxdepth 1 -type f ! -name SHA256SUMS | sort | xargs shasum -a 256 > SHA256SUMS
	@echo "  -> $(AUREKAI_SHA256SUMS)"

sbom: release
	@node -e " \
	  const fs = require('fs'); \
	  const pkg = JSON.parse(fs.readFileSync('$(CURDIR)/package.json','utf8')); \
	  const sbom = { \
	    spdxVersion: 'SPDX-2.3', \
	    SPDXID: 'SPDXRef-DOCUMENT', \
	    name: pkg.name + '-v$(AUREKAI_VERSION)', \
	    dataLicense: 'CC0-1.0', \
	    documentNamespace: 'https://aurekai.ai/sbom/' + pkg.name + '-v$(AUREKAI_VERSION)', \
	    creationInfo: { \
	      created: new Date().toISOString(), \
	      creators: ['Tool: aurekai-makefile'] \
	    }, \
	    packages: [{ \
	      SPDXID: 'SPDXRef-Package', \
	      name: pkg.name, \
	      versionInfo: '$(AUREKAI_VERSION)', \
	      downloadLocation: 'https://github.com/aurekai/aurekai/releases/tag/v$(AUREKAI_VERSION)', \
	      filesAnalyzed: false, \
	      licenseConcluded: pkg.license || 'NOASSERTION', \
	      licenseDeclared: pkg.license || 'NOASSERTION', \
	      copyrightText: 'NOASSERTION' \
	    }] \
	  }; \
	  fs.writeFileSync('$(AUREKAI_SBOM)', JSON.stringify(sbom, null, 2) + '\n'); \
	"
	@echo "  -> $(AUREKAI_SBOM)"

release-check:
	@$(MAKE) release HYPER_TARGET=$(HYPER_TARGET) MODEL_MEMORY_FAMILY=$(MODEL_MEMORY_FAMILY)
	@BONFYRE_RUNTIME=$(ROOT_DIR)/dist/bonfyre-appliance \
	BONFYRE_HOME=/tmp/aurekai-release-check-state \
	AKAI_HYPER=$(AUREKAI_HYPER) \
	node ./bin/akai.mjs doctor --deep --manifest $(LEGACY_COMPAT_MANIFEST)

clean:
	@rm -rf $(AUREKAI_DIST)