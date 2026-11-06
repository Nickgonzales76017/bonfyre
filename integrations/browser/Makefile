BUN ?= bun
ROOT := ../..
WEB_OUT ?= build/web-workbench
.PHONY: all prepare check test native-workspace package web-package test-workbench test-activity test-account-work test-provider-browser
all: prepare
prepare:
	$(BUN) $(ROOT)/scripts/project-recurring-commerce-browser.mjs --write
	$(BUN) $(ROOT)/scripts/project-provider-work-browser.mjs --write
	$(BUN) $(ROOT)/scripts/project-program-service-policy.mjs --write
check:
	$(BUN) $(ROOT)/scripts/project-recurring-commerce-browser.mjs --check
	$(BUN) $(ROOT)/scripts/project-provider-work-browser.mjs --check
	$(BUN) $(ROOT)/scripts/project-program-service-policy.mjs --check
test: check native-workspace test-workbench test-activity test-account-work test-programs
	$(BUN) test tests/commerce/observation.test.mjs tests/commerce/recurring-commerce.test.mjs tests/commerce/projection.test.mjs
test-workbench: check
	$(BUN) test tests/program-editor.test.mjs tests/workbench-actions.test.mjs tests/cycle-reschedule.test.mjs tests/contents.test.mjs tests/native-workbench-package.test.mjs tests/web-package.test.mjs tests/workbench-publish.test.mjs
test-activity:
	$(BUN) test tests/aurekai-activity-source.test.mjs tests/aurekai-activity.browser.test.mjs tests/foreign-sign-in.test.mjs tests/foreign-sign-in.browser.test.mjs tests/provider-continuation.test.mjs $(ROOT)/cmd/BonfyreSourceFrontier/tests/web_work.test.mjs $(ROOT)/cmd/BonfyreWebAddress/tests/activity_workspace.test.mjs $(ROOT)/cmd/BonfyreWebAddress/tests/source_web_work.test.mjs $(ROOT)/integrations/mcp/tests/fabric_activity.test.mjs $(ROOT)/integrations/mcp/tests/fabric_transport.test.mjs $(ROOT)/integrations/mcp/tests/fabric_store.test.mjs
test-account-work:
	$(BUN) test tests/aurekai-model.test.mjs tests/aurekai-model.browser.test.mjs tests/aurekai-push.test.mjs tests/aurekai-push.browser.test.mjs tests/aurekai-push-rail.browser.test.mjs $(ROOT)/integrations/mcp/tests/fabric_model.test.mjs $(ROOT)/integrations/mcp/tests/native_model.test.mjs $(ROOT)/integrations/mcp/tests/fabric_model_pipeline.test.mjs $(ROOT)/integrations/mcp/tests/native_meter.test.mjs $(ROOT)/integrations/mcp/tests/fabric_output_transfer.test.mjs $(ROOT)/integrations/mcp/tests/fabric_push.test.mjs
	$(BUN) test tests/activity-source.test.mjs tests/aurekai-inbox-source.test.mjs tests/aurekai-activity-interaction.browser.test.mjs $(ROOT)/cmd/BonfyreWebAddress/tests/account_activity_binding.test.mjs $(ROOT)/cmd/BonfyreWebAddress/tests/account_activity_web.test.mjs
test-provider-browser:
	$(BUN) $(ROOT)/scripts/project-provider-work-browser.mjs --check
	$(BUN) test tests/provider-continuation.browser.test.mjs
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
