TARGET = bonfyre-project
OWNER_DIR = ../../components/BonfyreProject
SYNC_BIN = $(HOME)/.bonfyre/bin/$(TARGET)

all: $(TARGET) release-convergence-tests public-foreign-release-witness public-foreign-live-witness civic-sync-witness
.PHONY: estate-material-witness
all: estate-material-witness
estate-material-witness:
	sh ../../scripts/tests/bonfyre_automate_test.sh
.PHONY: civic-live-readback
civic-live-readback: civic-sync-witness
	bun tools/civic_sync.mjs --cms-db $(HOME)/.bonfyre/moe/cms.db --schemas $(abspath ../../cmd/BonfyreCMS/content-types) --cms-bin $(HOME)/.bonfyre/bin/bonfyre-cms --project-bin $(CURDIR)/bonfyre-project --state $(HOME)/.bonfyre/moe/receipts/civic-sync --limit 12
.PHONY: civic-sync-witness
civic-sync-witness:
	bun test tests/civic_sync.test.mjs

# Disposable Git fixtures only; never modifies this source index or production.
release-convergence-tests:
	python3 -m unittest discover -s ../../scripts/release/tests -p 'test_*.py' -v

.PHONY: public-foreign-release-witness
public-foreign-release-witness:
	bun test ../../integrations/mcp/tests/project_native.test.mjs

.PHONY: public-foreign-live-witness
public-foreign-live-witness:
	bun tests/public_foreign_live_witness.mjs

# Let the owner track its generated contracts and native link dependencies.
# A second compile recipe here had drifted behind the component's source.
$(TARGET): FORCE
	$(MAKE) -C $(OWNER_DIR) $(TARGET)
	cp $(OWNER_DIR)/$(TARGET) $@

sync: $(TARGET)
	mkdir -p $(HOME)/.bonfyre/bin
	cp $(TARGET) $(SYNC_BIN)
	codesign -s - $(SYNC_BIN) >/dev/null 2>&1 || true

.PHONY: all sync clean FORCE release-convergence-tests public-foreign-release-witness
FORCE:

clean:
	rm -f $(TARGET)
