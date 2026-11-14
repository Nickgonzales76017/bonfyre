BUN ?= bun
ROOT := ../..
WEB_OUT ?= build/web-workbench
.PHONY: all prepare check test native-workspace package web-package test-workbench test-activity test-account-work test-provider-browser test-changed test-verification-contract
all: prepare
prepare:
	$(BUN) $(ROOT)/scripts/project-recurring-commerce-browser.mjs --write
	$(BUN) $(ROOT)/scripts/project-provider-work-browser.mjs --write
	$(BUN) $(ROOT)/scripts/project-program-service-policy.mjs --write
check:
	$(BUN) $(ROOT)/scripts/project-recurring-commerce-browser.mjs --check
	$(BUN) $(ROOT)/scripts/project-provider-work-browser.mjs --check
	$(BUN) $(ROOT)/scripts/project-program-service-policy.mjs --check
test: check test-verification-contract native-workspace test-workbench test-activity test-account-work test-programs
	$(BUN) test tests/commerce/observation.test.mjs tests/commerce/recurring-commerce.test.mjs tests/commerce/projection.test.mjs
test-verification-contract:
	@bash $(ROOT)/scripts/tests/test-gate.test.sh
test-changed: test-verification-contract
	@BONFYRE_BUN_BINARY="$(BUN)" bash $(ROOT)/scripts/verification/test-gate.sh run --git --fast
.PHONY: test-changed-full
test-changed-full: test-verification-contract
	@BONFYRE_BUN_BINARY="$(BUN)" bash $(ROOT)/scripts/verification/test-gate.sh run --git
test-workbench: check
	$(BUN) test tests/program-editor.test.mjs tests/workbench-actions.test.mjs tests/cycle-reschedule.test.mjs tests/contents.test.mjs tests/native-workbench-package.test.mjs tests/web-package.test.mjs tests/workbench-publish.test.mjs
test-activity:
	@BONFYRE_BUN_BINARY="$(BUN)" bash $(ROOT)/scripts/verification/test-gate.sh run --suite activity_unit --suite activity_browser --suite foreign_signin_browser
test-account-work:
	@BONFYRE_BUN_BINARY="$(BUN)" bash $(ROOT)/scripts/verification/test-gate.sh run --suite account_unit --suite model_browser --suite push_browser --suite push_rail_browser --suite inbox_unit --suite inbox_browser
test-provider-browser:
	$(BUN) $(ROOT)/scripts/project-provider-work-browser.mjs --check
	@BONFYRE_BUN_BINARY="$(BUN)" bash $(ROOT)/scripts/verification/test-gate.sh run --suite provider_browser
web-package: check
	$(BUN) web-package.mjs "$(WEB_OUT)"
.PHONY: publish-workbench verify-workbench
publish-workbench: test-workbench
	$(BUN) publish-workbench.mjs --publish
verify-workbench:
	$(BUN) publish-workbench.mjs --check
native-workspace:
	$(MAKE) -C $(ROOT)/cmd/BonfyreCMS bonfyre-cms
	$(MAKE) -C $(ROOT)/cmd/BonfyreWebAddress bonfyre-web-address
	$(BUN) test tests/native-workspace.test.mjs
.PHONY: test-enrollment-continuation
test-enrollment-continuation:
	$(MAKE) -C $(ROOT)/cmd/BonfyreQueue bonfyre-queue
	$(MAKE) -C $(ROOT)/cmd/BonfyreCredentials bonfyre-credentials
	@mkdir -p build
	@BONFYRE_QUEUE_BINARY="$(abspath $(ROOT)/cmd/BonfyreQueue/bonfyre-queue)" \
	BONFYRE_CREDENTIALS_BINARY="$(abspath $(ROOT)/cmd/BonfyreCredentials/bonfyre-credentials)" \
	$(BUN) test tests/browser-host.test.mjs tests/enrollment.test.mjs tests/enrollment-continuation.test.mjs \
	>build/enrollment-continuation.native.log 2>&1; result=$$?; \
	tail -n 90 build/enrollment-continuation.native.log; exit $$result
	@node --experimental-vm-modules --test tests/enrollment-material.test.mjs \
	tests/enrollment-fields.test.mjs tests/enrollment-integrity.test.mjs \
	tests/enrollment-panel.test.mjs tests/foreign-sign-in.test.mjs \
	>build/enrollment-continuation.panel.log 2>&1; result=$$?; \
	tail -n 70 build/enrollment-continuation.panel.log; exit $$result

package: prepare
	$(MAKE) check
	mkdir -p build
	@set -e; temp=$$(mktemp build/.extension.XXXXXX); \
	trap 'rm -f "$$temp"' EXIT INT TERM; \
	tar -C extension -czf "$$temp" .; \
	mv "$$temp" build/aurekai-extension.tar.gz

.PHONY: test-programs
test-programs:
	$(MAKE) -C $(ROOT)/cmd/BonfyreCMS bonfyre-cms
	$(BUN) $(ROOT)/scripts/project-program-service-policy.mjs --check
	$(MAKE) -C $(ROOT)/cmd/BonfyreCMS -f tests/program_continuation_test.mk test
	$(BUN) test tests/program-service-continuations.test.mjs
	$(BUN) test tests/program-workspace.browser.test.mjs $(ROOT)/cmd/BonfyreWebAddress/tests/program_workspace.test.mjs $(ROOT)/scripts/tests/program-workspace-owner.test.mjs $(ROOT)/scripts/tests/program-source-work.test.mjs
