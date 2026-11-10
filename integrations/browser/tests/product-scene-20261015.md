# Catalogue and room browser verification, 2026-10-15

## Verified work

The existing published catalogue grid and fixture planner were reconstructed with source-bound 3D adapters. Chrome completed the real product -> option -> quantity -> saved room -> 2D/3D coordinates -> alternative -> comparison -> review-preparation journey. This was an isolated verification server, not the public ServiceSite deployment.

The final run passes 34 browser assertions and 64 contract tests. Chrome 155.0.8059.39 used ANGLE/Vulkan SwiftShader software rendering. Actual licensed GLBs and actual Draco, Meshopt and Basis/KTX2 fixtures rendered. Tests include reload without another model fetch, one retained model body, stale-tab refusal, full browser-process restart, readable controls and hover state, 390-pixel layout and context-loss recovery back to 2D. No physical-phone, Safari or hardware-GPU performance claim is made.

Repository: https://github.com/Nickgonzales76017/bonfyre
Verification branch: verify/product-scene-20261015
Tested revision: 4245f2807b742a0f31fc7e50d86f3a87d4834daf
Run: https://github.com/Nickgonzales76017/bonfyre/actions/runs/38798475734
Evidence artifact: 12003835921
Artifact SHA-256: edabd4bb60723b7ed64b94f2bed3134380c06827aa8cfa0a594886cc079b45d4

## Applied canonical change

`integrations/browser/extension/store.mjs` now performs prefix reads with an IndexedDB cursor instead of loading every record before filtering. The existing database and revision-checked writes remain unchanged. This prevents each small draft lookup from first loading every unrelated model body.

Patch receipt: patch-1792094648-01218d7eb370
Before SHA-256: a906f1abb54051c4de9d1db0d6e21b0923d56202bd2193607d3a5dae4baf36d6
After SHA-256: 234cc8be0469c8b1bbbf16ba4fb86e18bfa06b6915c6dd8de88d36ccd41d608c
The after hash is identical to the browser-tested source. Native node --check passed. The browser-store change has not been deployed to the public site.

## Source and commercial boundaries

Wayfair W002337865 is the real commercial source. The current listing calls it Bonsall Table Lamp; the licensed IridescenceLamp README links the same source as Bonsell 19-inch Table Lamp. Observed price is $69.99, with Chrome base and Gray shade. This is one observed configuration, not proof of multiple merchant variants. Two, three and four units produced $139.98, $209.97 and $279.96. Conflicting source depth dimensions remain a review question.

IridescenceLamp is copyright 2022 Wayfair LLC / Eric Chadwick, CC BY 4.0. SheenChair is copyright 2020 Wayfair LLC / Eric Chadwick, CC0; its author explicitly says it is not a real product. Its two material variants are renderer tests and are not mapped to RED3 SKUs. Asset source revision is edc7c9e67c639d230715049ee31f9a96a6babbbe in KhronosGroup/glTF-Sample-Assets.

## Remaining integration

ServiceSite component writes remain outside the automatic writer's allowed roots. No permission policy, credential, private staged file, public page or merchant record was changed to bypass that boundary.

The pending runtime adapters are in the verification branch's hashed source packet and refinement script. They still need normal ServiceSite section/material packaging and source admission. In particular, opaque source SKU identifiers must be supported consistently instead of weakening exact source-membership checks. The isolated Wayfair collection has not been admitted by the production material compiler.

Native review deliberately returned 503 in the browser test. The original review screen, brief and submission payload were exercised, but no accepted native receipt or downstream CMS delivery is claimed. Account-scoped sharing, review decisions and merchant publication remain open. Use the existing native lead/Auth/CMS owners for those steps, not a static success response or a second application store.

## Public releases, later on 2026-10-15

The earlier sections describe the isolated verification checkpoint. The application is now published through the existing GitHub Pages showcase. Cloudflare Work Samples and the protected ServiceSite components on the Mac remain unchanged by this release.

Catalogue: https://nickgonzales76017.github.io/bonfyre/spaces/catalogue/?store=wayfair-reference&workspace=inventory
Room: https://nickgonzales76017.github.io/bonfyre/spaces/product-room/
Showcase: https://nickgonzales76017.github.io/bonfyre/#experiences

Baseline publication and live browser verification passed in run 38803993494. The helper-backed upgrade then passed live in run 38806953421. Final polished publication and discovery verification passed in run 38810193140, with 28 preflight and 30 actual public-browser assertions. The final evidence archive is artifact 12008597513, SHA-256 2c40152871a39d55333207c404be82362b9a2e44de47256e4a136b2160a4f0ed. Its spaces-live-evidence/RESULTS.txt and screenshots are the public witness; the original 64 contract tests remain a separate earlier verification scope.

Final workflow: https://github.com/Nickgonzales76017/bonfyre/actions/runs/38810193140
Verification revision: cc049fdb5fb56ed365150d6e4d13cd80a448720b. The workflow subsequently retained its generated release and screenshots with a separate bot commit before deploying that working tree.

The published view uses camera-controls 3.1.2, @pmndrs/vanilla 1.25.0 Grid and PCSS helpers, and the existing Three r180 postprocessing, CSS2D and ViewHelper addons. Licences and dependencies are retained in the public repository. Mesh-part isolation, restored whole-product views, exploded inspection, projected dimensions, lighting, expanded workspace, direct pointer placement and existing undo were exercised on rendered models. Expanded inspection spans are distinguished from original model dimensions.

Selection and placement changes reuse existing model instances. Both operations were checked against the existing plan records and revision-checked browser store. The UI is published in content-addressed module packages without changing catalogue or saved-plan identity. The final public journey sends no POST requests. Two existing showcase cards now point to the live catalogue and room routes, and their actual application screenshots were loaded in the browser.

Public source: components/BonfyreServiceSite/product-scene-tools.mjs; scripts/upgrade-spaces.mjs; scripts/prepare-spaces-discovery.mjs; scripts/tests/spaces-live-browser.mjs; scripts/tests/spaces-studio-checks.mjs; .github/workflows/pages.yml in Nickgonzales76017/bonfyre main. These public source paths are not claims that the protected local ServiceSite component files have been changed. The browser-store prefix fix is included in the published projection.

Review remains preparation, not accepted native delivery. The published static surface does not collect contact details or claim a receipt; its source-bearing brief remains available. Normal ServiceSite material compilation/source admission, shared authenticated work, native reviewer decisions and merchant publication remain open. No private convergence branch was merged, no native permission was broadened, and no supplier was contacted. Physical-phone, Safari and hardware-GPU testing remain unperformed.
