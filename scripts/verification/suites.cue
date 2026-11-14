package verification
#Suite: {
 owner: string
 kind: "unit" | "browser" | "vector"
 build?: string
 target?: string
 files: [string, ...string]
 watches: [...string]
}
globalWatches: ["integrations/browser/Makefile","integrations/browser/workspace-msgpack.mjs","integrations/browser/provider-contract.mjs","scripts/verification/","architecture/","lib/libbonfyre/","cmd/BonfyreWebAddress/src/","cmd/BonfyreCMS/src/"]
suites: {
 activity_unit: #Suite & {
  owner: "BonfyreActivity"
  kind: "unit"
  watches: ["integrations/browser/aurekai-activity-source.mjs","integrations/browser/foreign-sign-in.mjs","integrations/browser/provider-continuation.mjs","integrations/mcp/fabric_*","cmd/BonfyreSourceFrontier/tools/web_work.mjs"]
  files: ["integrations/browser/tests/aurekai-activity-source.test.mjs","integrations/browser/tests/foreign-sign-in.test.mjs","integrations/browser/tests/provider-continuation.test.mjs","cmd/BonfyreSourceFrontier/tests/web_work.test.mjs","cmd/BonfyreWebAddress/tests/activity_workspace.test.mjs","cmd/BonfyreWebAddress/tests/source_web_work.test.mjs","integrations/mcp/tests/fabric_activity.test.mjs","integrations/mcp/tests/fabric_transport.test.mjs","integrations/mcp/tests/fabric_store.test.mjs"]
 }
 activity_browser: #Suite & {
  owner: "BonfyreActivity"
  kind: "browser"
  watches: ["integrations/browser/aurekai-activity.mjs","integrations/browser/aurekai-activity.css","integrations/browser/aurekai-inbox-source.mjs"]
  files: ["integrations/browser/tests/aurekai-activity.browser.test.mjs"]
 }
 foreign_signin_browser: #Suite & {
  owner: "BonfyreProvider"
  kind: "browser"
  watches: ["integrations/browser/extension/foreign-sign-in.mjs","integrations/browser/extension/foreign-sign-in.css"]
  files: ["integrations/browser/tests/foreign-sign-in.browser.test.mjs"]
 }
 account_unit: #Suite & {
  owner: "BonfyreModel"
  kind: "unit"
  watches: ["integrations/browser/aurekai-model.mjs","integrations/browser/aurekai-push.mjs","integrations/mcp/fabric_model","integrations/mcp/native_model*","integrations/mcp/native_meter*","integrations/mcp/fabric_push*"]
  files: ["integrations/browser/tests/aurekai-model.test.mjs","integrations/browser/tests/aurekai-push.test.mjs","integrations/mcp/tests/fabric_model.test.mjs","integrations/mcp/tests/native_model.test.mjs","integrations/mcp/tests/fabric_model_pipeline.test.mjs","integrations/mcp/tests/native_meter.test.mjs","integrations/mcp/tests/fabric_output_transfer.test.mjs","integrations/mcp/tests/fabric_push.test.mjs"]
 }
 model_browser: #Suite & {
  owner: "BonfyreModel"
  kind: "browser"
  watches: ["integrations/browser/aurekai-model.mjs","integrations/browser/aurekai-model.css"]
  files: ["integrations/browser/tests/aurekai-model.browser.test.mjs"]
 }
 push_browser: #Suite & {
  owner: "BonfyrePush"
  kind: "browser"
  watches: ["integrations/browser/aurekai-push.mjs","integrations/browser/aurekai-push-sw.mjs"]
  files: ["integrations/browser/tests/aurekai-push.browser.test.mjs"]
 }
 push_rail_browser: #Suite & {
  owner: "BonfyrePush"
  kind: "browser"
  watches: ["integrations/browser/aurekai-push.mjs","integrations/browser/aurekai-activity.mjs"]
  files: ["integrations/browser/tests/aurekai-push-rail.browser.test.mjs"]
 }
 inbox_unit: #Suite & {
  owner: "BonfyreInbox"
  kind: "unit"
  watches: ["integrations/browser/activity-source.mjs","integrations/browser/aurekai-inbox-source.mjs","cmd/BonfyreWebAddress/src/account_*"]
  files: ["integrations/browser/tests/activity-source.test.mjs","integrations/browser/tests/aurekai-inbox-source.test.mjs","cmd/BonfyreWebAddress/tests/account_activity_binding.test.mjs","cmd/BonfyreWebAddress/tests/account_activity_web.test.mjs"]
 }
 inbox_browser: #Suite & {
  owner: "BonfyreInbox"
  kind: "browser"
  watches: ["integrations/browser/aurekai-activity.mjs","integrations/browser/aurekai-inbox-source.mjs"]
  files: ["integrations/browser/tests/aurekai-activity-interaction.browser.test.mjs"]
 }
 provider_browser: #Suite & {
  owner: "BonfyreProvider"
  kind: "browser"
  watches: ["integrations/browser/provider-continuation.mjs","integrations/browser/extension/foreign-sign-in.mjs"]
  files: ["integrations/browser/tests/provider-continuation.browser.test.mjs"]
 }
 finance_vectors: #Suite & {
  owner: "BonfyreFinance"
  kind: "vector"
  build: "cmd/BonfyreFinance"
  target: "bonfyre-finance"
  watches: ["cmd/BonfyreFinance/src/","cmd/BonfyreFinance/tests/vectors/","cmd/BonfyreFinance/Makefile"]
  files: [
   "cmd/BonfyreFinance/tests/vectors/payment_options.json",
   "cmd/BonfyreFinance/tests/vectors/relationship_states.json",
   "cmd/BonfyreFinance/tests/vectors/supplier_quote_memo.json",
   "cmd/BonfyreFinance/tests/vectors/zero_close.json",
   "cmd/BonfyreFinance/tests/vectors/opportunity_inspect.json",
  ]
 }
 experience_vectors: #Suite & {
  owner: "BonfyreExperienceMap"
  kind: "vector"
  build: "cmd/BonfyreExperienceMap"
  target: "bonfyre-experience-map"
  watches: ["cmd/BonfyreExperienceMap/src/","cmd/BonfyreExperienceMap/tests/vectors/","cmd/BonfyreExperienceMap/Makefile"]
  files: [
   "cmd/BonfyreExperienceMap/tests/vectors/native_definition.json",
   "cmd/BonfyreExperienceMap/tests/vectors/native_resolution.json",
   "cmd/BonfyreExperienceMap/tests/vectors/policy_compilation.json",
  ]
 }
 finance_unit: #Suite & {
  owner: "BonfyreFinance"
  kind: "unit"
  build: "cmd/BonfyreFinance"
  target: "bonfyre-finance"
  watches: ["cmd/BonfyreFinance/src/","cmd/BonfyreFinance/tests/","cmd/BonfyreFinance/Makefile"]
  files: ["cmd/BonfyreFinance/tests/opportunity_inspect.test.mjs","cmd/BonfyreFinance/tests/crm_finance_auth.test.mjs"]
 }
 finance_browser: #Suite & {
  owner: "BonfyreFinance"
  kind: "browser"
  build: "cmd/BonfyreFinance"
  target: "bonfyre-finance"
  watches: ["cmd/BonfyreFinance/src/","cmd/BonfyreFinance/tests/crm_finance_browser.test.mjs"]
  files: ["cmd/BonfyreFinance/tests/crm_finance_browser.test.mjs"]
 }
 transcribe_caption_vectors: #Suite & {
  owner: "BonfyreTranscribe"
  kind: "vector"
  build: "cmd/BonfyreTranscribe"
  target: "bonfyre-transcribe"
  watches: ["cmd/BonfyreTranscribe/src/","cmd/BonfyreTranscribe/tests/vectors/","cmd/BonfyreTranscribe/Makefile","cmd/BonfyreMediaPrep/src/"]
  files: ["cmd/BonfyreTranscribe/tests/vectors/caption_contract.json"]
 }
 queue_vectors: #Suite & {
  owner: "BonfyreQueue"
  kind: "vector"
  build: "cmd/BonfyreQueue"
  target: "bonfyre-queue"
  watches: ["cmd/BonfyreQueue/src/","cmd/BonfyreQueue/Makefile","tests/vectors/plane/"]
  files: ["tests/vectors/plane/queue_behavior.json"]
 }
 finance_subtools: #Suite & {
  owner: "BonfyreWebAddress"
  kind: "browser"
  build: "cmd/BonfyreWebAddress"
  target: "bonfyre-web-address"
  watches: ["cmd/BonfyreFinance/src/","cmd/BonfyreWebAddress/tests/installed_subtools_test.mjs","cmd/BonfyreWebAddress/src/credit_profile_projection.h","cmd/BonfyreWebAddress/src/application_continuation.h","integrations/browser/source-desk.mjs","integrations/browser/workspace-destinations.mjs"]
  files: ["cmd/BonfyreWebAddress/tests/installed_subtools_test.mjs"]
 }
}
